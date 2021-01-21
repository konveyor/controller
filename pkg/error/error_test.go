package error

import (
	"errors"
	"github.com/onsi/gomega"
	errors2 "github.com/pkg/errors"
	"testing"
)

func TestError(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	err := errors.New("failed")
	le := Wrap(err).(*Error)
	g.Expect(le).NotTo(gomega.BeNil())
	g.Expect(le.wrapped).To(gomega.Equal(err))
	g.Expect(len(le.stack)).To(gomega.Equal(4))
	g.Expect(le.Error()).To(gomega.Equal(err.Error()))

	le2 := Wrap(err).(*Error)
	g.Expect(le2).NotTo(gomega.BeNil())
	g.Expect(le2.wrapped).To(gomega.Equal(err))
	g.Expect(len(le2.stack)).To(gomega.Equal(4))
	g.Expect(le2.Error()).To(gomega.Equal(err.Error()))

	wrapped := errors2.Wrap(err, "help")
	le3 := Wrap(wrapped).(*Error)
	g.Expect(le3).NotTo(gomega.BeNil())
	g.Expect(le3.wrapped).To(gomega.Equal(wrapped))
	g.Expect(le3.wrapped).To(gomega.Equal(wrapped))
	g.Expect(len(le3.stack)).To(gomega.Equal(4))
	g.Expect(errors.Unwrap(le3)).To(gomega.Equal(err))

	println(le.Stack())
}

func TestUnwrap(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	err := errors.New("failed")
	g.Expect(err).To(gomega.Equal(Unwrap(err)))
	g.Expect(Unwrap(nil)).To(gomega.BeNil())
	g.Expect(Unwrap(Wrap(err))).To(gomega.Equal(err))
	g.Expect(Unwrap(errors2.Wrap(err, ""))).To(gomega.Equal(err))
	g.Expect(Unwrap(errors2.Wrap(errors2.Wrap(err, ""), ""))).To(gomega.Equal(err))
}
