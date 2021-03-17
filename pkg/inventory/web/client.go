package web

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/websocket"
	liberr "github.com/konveyor/controller/pkg/error"
	libmodel "github.com/konveyor/controller/pkg/inventory/model"
	"io/ioutil"
	"net/http"
	liburl "net/url"
	"reflect"
	"time"
)

//
// Header.
const (
	WatchHeader = "X-Watch"
)

//
// Event handler
type EventHandler interface {
	// The watch has started.
	Started()
	// Parity marker.
	// The watch has delivered the initial set
	// of `Created` events.
	Parity()
	// Resource created.
	Created(r Event)
	// Resource updated.
	Updated(r Event)
	// Resource deleted.
	Deleted(r Event)
	// An error has occurred.
	Error(error)
	// The watch has ended.
	End()
}

//
// REST client.
type Client struct {
	// Transport.
	Transport http.RoundTripper
	// Headers.
	Header http.Header
}

//
// HTTP GET (method).
func (r *Client) Get(url string, out interface{}) (status int, err error) {
	parsedURL, err := liburl.Parse(url)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	request := &http.Request{
		Header: r.Header,
		Method: http.MethodGet,
		URL:    parsedURL,
	}
	client := http.Client{Transport: r.Transport}
	response, err := client.Do(request)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	status = response.StatusCode
	content := []byte{}
	if status == http.StatusOK {
		defer func() {
			_ = response.Body.Close()
		}()
		content, err = ioutil.ReadAll(response.Body)
		if err != nil {
			err = liberr.Wrap(err)
			return
		}
		err = json.Unmarshal(content, out)
		if err != nil {
			err = liberr.Wrap(err)
			return
		}
	}

	return
}

//
// HTTP POST (method).
func (r *Client) Post(url string, in interface{}, out interface{}) (status int, err error) {
	parsedURL, err := liburl.Parse(url)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	body, _ := json.Marshal(in)
	reader := bytes.NewReader(body)
	request := &http.Request{
		Header: r.Header,
		Method: http.MethodPost,
		Body:   ioutil.NopCloser(reader),
		URL:    parsedURL,
	}
	client := http.Client{Transport: r.Transport}
	response, err := client.Do(request)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	status = response.StatusCode
	content := []byte{}
	if status == http.StatusOK {
		defer func() {
			_ = response.Body.Close()
		}()
		if out == nil {
			return
		}
		content, err = ioutil.ReadAll(response.Body)
		if err != nil {
			err = liberr.Wrap(err)
			return
		}
		err = json.Unmarshal(content, out)
		if err != nil {
			err = liberr.Wrap(err)
			return
		}
	}

	return
}

//
// Watch a resource.
func (r *Client) Watch(
	url string,
	resource interface{},
	handler EventHandler) (status int, w *Watch, err error) {
	//
	url = r.patchURL(url)
	dialer := websocket.DefaultDialer
	post := func(w *WatchReader) (pStatus int, pErr error) {
		socket, response, pErr := dialer.Dial(
			url, http.Header{
				WatchHeader: []string{"1"},
			})
		if pErr != nil {
			pErr = liberr.Wrap(pErr)
			return
		}
		pStatus = response.StatusCode
		switch pStatus {
		case http.StatusOK,
			http.StatusSwitchingProtocols:
			pStatus = http.StatusOK
			w.webSocket = socket
		}
		return
	}
	reader := &WatchReader{
		resource: resource,
		handler:  handler,
		repair:   post,
	}
	status, err = post(reader)
	if err != nil || status != http.StatusOK {
		return
	}

	w = &Watch{reader: reader}
	reader.start()

	return
}

//
// Patch the URL.
func (r *Client) patchURL(in string) (out string) {
	out = in
	url, err := liburl.Parse(in)
	if err != nil {
		return
	}
	switch url.Scheme {
	case "http":
		url.Scheme = "ws"
	case "https":
		url.Scheme = "wss"
	default:
		return
	}

	out = url.String()

	return
}

//
// Watch (event) reader.
type WatchReader struct {
	// Repair function.
	repair func(*WatchReader) (int, error)
	// Web socket.
	webSocket *websocket.Conn
	// Web resource.
	resource interface{}
	// Event handler.
	handler EventHandler
	// Started.
	started bool
	// Done.
	done bool
}

//
// Dispatch events.
func (r *WatchReader) start() {
	if r.started {
		return
	}
	r.started = true
	r.done = false
	go func() {
		defer func() {
			_ = r.webSocket.Close()
		}()
		r.handler.Started()
		for {
			event := Event{
				Resource: r.clone(r.resource),
				Updated:  r.clone(r.resource),
			}
			err := r.webSocket.ReadJSON(&event)
			if err != nil {
				if r.done {
					break
				}
				r.handler.Error(err)
				for {
					time.Sleep(time.Second * 10)
					status, err := r.repair(r)
					if err != nil || status != http.StatusOK {
						r.handler.Error(err)
					} else {
						break
					}
				}
			}
			switch event.Action {
			case libmodel.Parity:
				r.handler.Parity()
			case libmodel.Created:
				r.handler.Created(event)
			case libmodel.Updated:
				r.handler.Updated(event)
			case libmodel.Deleted:
				r.handler.Deleted(event)
			}
		}
		r.started = false
		r.handler.End()
	}()
}

//
// Clone resource.
func (r *WatchReader) clone(in interface{}) (out interface{}) {
	mt := reflect.TypeOf(in)
	mv := reflect.ValueOf(in)
	switch mt.Kind() {
	case reflect.Ptr:
		mt = mt.Elem()
		mv = mv.Elem()
	}
	new := reflect.New(mt).Elem()
	new.Set(mv)
	return new.Addr().Interface()
}

//
// Terminate.
func (r *WatchReader) terminate() {
	r.done = true
	_ = r.webSocket.Close()
}

//
// Represents a watch.
type Watch struct {
	reader *WatchReader
}

//
// End the watch.
func (r *Watch) End() {
	r.reader.terminate()
}
