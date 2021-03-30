package web

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/onsi/gomega"
	auth "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
	"time"
)

type fakeWriter struct {
	valid bool
	count int
}

func (r *fakeWriter) Create(
	ctx context.Context,
	object runtime.Object) error {
	//
	r.count++
	object.(*auth.TokenReview).Status.Authenticated = r.valid
	return nil
}

func (r *fakeWriter) Delete(
	context.Context,
	runtime.Object,
	...client.DeleteOptionFunc) error {
	return nil
}

func (r *fakeWriter) Update(
	context.Context,
	runtime.Object) error {
	return nil
}

func TestAuth(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	ttl := time.Millisecond * 50
	writer := &fakeWriter{valid: true}
	auth := Authenticator{
		Writer: writer,
		TTL:    ttl,
	}
	token := "12345"
	ctx := &gin.Context{
		Request: &http.Request{
			Header: map[string][]string{
				"Authorization": {"Bearer " + token},
			},
		},
	}
	// token.
	g.Expect(auth.token(ctx)).To(gomega.Equal(token))
	// First call with no cached token.
	auth.Authenticate(ctx)
	g.Expect(auth.cache[token]).ToNot(gomega.BeNil())
	g.Expect(1).To(gomega.Equal(writer.count))
	// Second call with cached token.
	auth.Authenticate(ctx)
	g.Expect(1).To(gomega.Equal(writer.count))
	// Third call after TTL.
	time.Sleep(ttl)
	auth.Authenticate(ctx)
	g.Expect(2).To(gomega.Equal(writer.count))
	// Prune
	auth.prune()
	g.Expect(auth.cache[token]).ToNot(gomega.BeNil())
	time.Sleep(ttl * 2)
	auth.prune()
	g.Expect(0).To(gomega.Equal(len(auth.cache)))
}
