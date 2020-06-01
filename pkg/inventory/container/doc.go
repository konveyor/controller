// Container
//   |__Reconciler
//   |__Reconciler
//   |__Reconciler
//
package container

import (
	"github.com/konveyor/controller/pkg/logging"
)

var Log *logging.Logger

func init() {
	log := logging.WithName("container")
	log.Reset()
	Log = &log
}

//
// Build a new container.
func New() *Container {
	return &Container{
		content: map[Key]Reconciler{},
	}
}
