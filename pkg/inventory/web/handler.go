package web

import (
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	liberr "github.com/konveyor/controller/pkg/error"
	"github.com/konveyor/controller/pkg/inventory/container"
	"github.com/konveyor/controller/pkg/inventory/model"
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
	wait := time.Second * 30
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
	// Action.
	Action uint8
	// Affected Resource.
	Resource interface{}
	// Updated resource.
	Updated interface{}
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
}

//
// End.
func (r *WatchWriter) end() {
	_ = r.webSocket.Close()
	r.watch.End()
}

//
// Write event to the socket.
func (r *WatchWriter) send(e model.Event) {
	event := Event{
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
		r.end()
	}
}

//
// Watch has started.
func (r *WatchWriter) Started() {
}

//
// Watch has parity.
func (r *WatchWriter) Parity() {
	r.send(model.Event{Action: model.Parity})
}

//
// A model has been created.
func (r *WatchWriter) Created(event model.Event) {
	r.send(event)
}

//
// A model has been updated.
func (r *WatchWriter) Updated(event model.Event) {
	r.send(event)
}

//
// A model has been deleted.
func (r *WatchWriter) Deleted(event model.Event) {
	r.send(event)
}

//
// An error has occurred delivering an event.
func (r *WatchWriter) Error(err error) {
	r.send(model.Event{Action: model.Error})
}

//
// An event watch has ended.
func (r *WatchWriter) End() {
	r.send(model.Event{Action: model.End})
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
	writer := &WatchWriter{builder: rb}
	socket, err := upGrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}
	writer.webSocket = socket
	writer.watch, err = db.Watch(m, writer)
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
