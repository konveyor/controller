package web

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"
	"github.com/gorilla/websocket"
	liberr "github.com/konveyor/controller/pkg/error"
	"github.com/konveyor/controller/pkg/inventory/container"
	"github.com/konveyor/controller/pkg/inventory/model"
	"github.com/konveyor/controller/pkg/logging"
	"github.com/konveyor/controller/pkg/ref"
	"net/http"
	"strconv"
	"time"
)

//
// Web request handler.
type RequestHandler interface {
	// Add routes to the `gin` router.
	AddRoutes(*gin.Engine)
	// List resources in a REST collection.
	List(*gin.Context)
	// Get a specific REST resource.
	Get(*gin.Context)
}

//
// Paged handler.
type Paged struct {
	// The `page` parameter passed in the request.
	Page model.Page
}

//
// Prepare the handler to fulfil the request.
// Set the `page` field using passed parameters.
func (h *Paged) Prepare(ctx *gin.Context) int {
	status := h.setPage(ctx)
	if status != http.StatusOK {
		return status
	}

	return http.StatusOK
}

//
// Set the `page` field.
func (h *Paged) setPage(ctx *gin.Context) int {
	q := ctx.Request.URL.Query()
	page := model.Page{
		Limit:  int(^uint(0) >> 1),
		Offset: 0,
	}
	pLimit := q.Get("limit")
	if len(pLimit) != 0 {
		nLimit, err := strconv.Atoi(pLimit)
		if err != nil || nLimit < 0 {
			return http.StatusBadRequest
		}
		page.Limit = nLimit
	}
	pOffset := q.Get("offset")
	if len(pOffset) != 0 {
		nOffset, err := strconv.Atoi(pOffset)
		if err != nil || nOffset < 0 {
			return http.StatusBadRequest
		}
		page.Offset = nOffset
	}

	h.Page = page
	return http.StatusOK
}

//
// Parity (not-partial) request handler.
type Parity struct {
}

//
// Ensure reconciler has achieved parity.
func (c *Parity) EnsureParity(r container.Reconciler, w time.Duration) int {
	wait := w
	poll := time.Microsecond * 100
	for {
		mark := time.Now()
		if r.HasParity() {
			return http.StatusOK
		}
		if wait > 0 {
			time.Sleep(poll)
			wait -= time.Since(mark)
		} else {
			break
		}
	}

	return http.StatusPartialContent
}

//
// Watched resource builder.
type ResourceBuilder func(model.Model) interface{}

//
// Event
type Event struct {
	// ID
	ID uint64
	// Action.
	Action uint8
	// Affected Resource.
	Resource interface{}
	// Updated resource.
	Updated interface{}
}

//
// String representation.
func (r *Event) String() string {
	action := "unknown"
	switch r.Action {
	case model.Started:
		action = "started"
	case model.Parity:
		action = "parity"
	case model.Error:
		action = "error"
	case model.End:
		action = "end"
	case model.Created:
		action = "created"
	case model.Updated:
		action = "updated"
	case model.Deleted:
		action = "deleted"
	}
	kind := ""
	if r.Resource != nil {
		kind = ref.ToKind(r.Resource)
	}
	return fmt.Sprintf(
		"event-%.4d: %s kind=%s",
		r.ID,
		action,
		kind)
}

//
// Watch (event) writer.
type WatchWriter struct {
	// negotiated web socket.
	webSocket *websocket.Conn
	// model watch.
	watch *model.Watch
	// Resource.
	builder ResourceBuilder
	// Logger.
	log logr.Logger
}

//
// End.
// Close the socket.
// End the watch.
// Reset the watch (pointer) to release both
// objects for garbage collection.
func (r *WatchWriter) end() {
	_ = r.webSocket.Close()
	r.watch.End()
	r.watch = &model.Watch{}
	r.log.V(3).Info("watch ended.")
}

//
// Write event to the socket.
func (r *WatchWriter) send(e model.Event) {
	event := Event{
		ID:     e.ID,
		Action: e.Action,
	}
	if e.Model != nil {
		event.Resource = r.builder(e.Model)
	}
	if e.Updated != nil {
		event.Updated = r.builder(e.Updated)
	}
	err := r.webSocket.WriteJSON(event)
	if err != nil {
		r.log.V(4).Error(err, "websocket send failed.")
		r.end()
	}

	r.log.V(5).Info(
		"event sent.",
		"event",
		event)
}

//
// Watch has started.
func (r *WatchWriter) Started(watchID uint64) {
	r.log.V(4).Info(
		"event: started.",
		"watch",
		watchID)
	r.send(model.Event{
		ID:     watchID, // send watch ID.
		Action: model.Started,
	})
}

//
// Watch has parity.
func (r *WatchWriter) Parity() {
	r.log.V(4).Info("event: parity.")
	r.send(model.Event{
		Action: model.Parity,
	})
}

//
// A model has been created.
func (r *WatchWriter) Created(event model.Event) {
	r.log.V(5).Info(
		"event received.",
		"event",
		event.String())
	r.send(event)
}

//
// A model has been updated.
func (r *WatchWriter) Updated(event model.Event) {
	r.log.V(5).Info(
		"event received.",
		"event",
		event.String())
	r.send(event)
}

//
// A model has been deleted.
func (r *WatchWriter) Deleted(event model.Event) {
	r.log.V(5).Info(
		"event received.",
		"event",
		event.String())
	r.send(event)
}

//
// An error has occurred delivering an event.
func (r *WatchWriter) Error(err error) {
	r.log.V(4).Info(
		"event: error",
		"error",
		err.Error())
	r.send(model.Event{
		Action: model.Error,
	})
}

//
// An event watch has ended.
func (r *WatchWriter) End() {
	r.log.V(4).Info("event: ended.")
	r.send(model.Event{
		Action: model.End,
	})
}

//
// Watched (handler).
type Watched struct {
	WatchRequest bool
}

//
// Prepare the handler to fulfil the request.
// Set the `HasWatch` field using passed headers.
func (h *Watched) Prepare(ctx *gin.Context) int {
	_, h.WatchRequest = ctx.Request.Header[WatchHeader]
	return http.StatusOK
}

//
// Watch model.
func (r *Watched) Watch(
	ctx *gin.Context,
	db model.DB,
	m model.Model,
	rb ResourceBuilder) (err error) {
	//
	upGrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	socket, err := upGrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		err = liberr.Wrap(
			err,
			"websocket upgrade failed.",
			"url",
			ctx.Request.URL)
		return
	}
	wlog := logging.WithName("web|watch|writer").WithValues(
		"peer",
		socket.RemoteAddr(),
		"model",
		ref.ToKind(m))
	writer := &WatchWriter{
		webSocket: socket,
		builder:   rb,
		log:       wlog,
	}
	watch, err := db.Watch(m, writer)
	if err != nil {
		_ = socket.Close()
		return
	}

	writer.watch = watch

	log.V(3).Info(
		"handler: watch created.",
		"url",
		ctx.Request.URL)

	return
}

//
// Schema (route) handler.
type SchemaHandler struct {
	// The `gin` router.
	router *gin.Engine
	// Schema version
	Version string
	// Schema release.
	Release int
}

//
// Add routes.
func (h *SchemaHandler) AddRoutes(r *gin.Engine) {
	r.GET("/schema", h.List)
	h.router = r
}

//
// List schema.
func (h *SchemaHandler) List(ctx *gin.Context) {
	type Schema struct {
		Version string   `json:"version,omitempty"`
		Release int      `json:"release,omitempty"`
		Paths   []string `json:"paths"`
	}
	schema := Schema{
		Version: h.Version,
		Release: h.Release,
		Paths:   []string{},
	}
	for _, rte := range h.router.Routes() {
		schema.Paths = append(schema.Paths, rte.Path)
	}

	ctx.JSON(http.StatusOK, schema)
}

//
// Not supported.
func (h SchemaHandler) Get(ctx *gin.Context) {
	ctx.Status(http.StatusMethodNotAllowed)
}
