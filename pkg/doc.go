package pkg

import (
	"github.com/konveyor/controller/pkg/inventory/container"
	"github.com/konveyor/controller/pkg/inventory/model"
	"github.com/konveyor/controller/pkg/inventory/web"
	"github.com/konveyor/controller/pkg/logging"
)

//go:generate go run ../vendor/k8s.io/code-generator/cmd/deepcopy-gen/main.go -O zz_generated.deepcopy -i ./... -h ../hack/boilerplate.go.txt

//
// Set loggers.
func SetLogger(logger *logging.Logger) {
	container.Log = logger
	model.Log = logger
	web.Log = logger
}
