package web

import "github.com/konveyor/controller/pkg/logging"

var Log *logging.Logger

func init() {
	log := logging.WithName("model")
	log.Reset()
	Log = &log
}
