package web

import (
	"context"
	"github.com/gin-gonic/gin"
	liberr "github.com/konveyor/controller/pkg/error"
	auth "k8s.io/api/authentication/v1"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"sync"
	"time"
)

//
// Authorized by k8s bearer token review.
type Authenticator struct {
	// k8s client.
	client.Writer
	// Cached token TTL.
	TTL time.Duration
	// Mutex.
	mutex sync.Mutex
	// Token cache.
	cache map[string]time.Time
}

//
// Authenticate token.
func (r *Authenticator) Authenticate(ctx *gin.Context) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.cache == nil {
		r.cache = make(map[string]time.Time)
	}
	r.prune()
	token := r.token(ctx)
	if token == "" {
		ctx.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	if t, found := r.cache[token]; found {
		if time.Since(t) <= r.TTL {
			ctx.Next()
			return
		}
	}
	valid, err := r.authenticate(token)
	if err != nil {
		ctx.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	if valid {
		r.cache[token] = time.Now()
	} else {
		delete(r.cache, token)
	}

	ctx.Next()
}

//
// Authenticate token.
func (r *Authenticator) authenticate(token string) (valid bool, err error) {
	tr := &auth.TokenReview{
		Spec: auth.TokenReviewSpec{
			Token: token,
		},
	}
	err = r.Create(context.TODO(), tr)
	if err != nil {
		err = liberr.Wrap(err)
		return
	}

	valid = tr.Status.Authenticated
	return
}

//
// Extract token.
func (r *Authenticator) token(ctx *gin.Context) (token string) {
	header := ctx.GetHeader("Authorization")
	fields := strings.Fields(header)
	if len(fields) == 2 && fields[0] == "Bearer" {
		token = fields[1]
	}

	return
}

//
// Prune the cache.
// Evacuate expired tokens.
func (r *Authenticator) prune() {
	for token, t := range r.cache {
		if time.Since(t) > r.TTL {
			delete(r.cache, token)
		}
	}
}
