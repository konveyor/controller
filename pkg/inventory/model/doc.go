package model

import "github.com/konveyor/controller/pkg/logging"

var Log *logging.Logger

func init() {
	log := logging.WithName("model")
	log.Reset()
	Log = &log
}

//
// New database.
func New(path string, models ...interface{}) DB {
	return &Client{
		path:   path,
		models: models,
	}
}
