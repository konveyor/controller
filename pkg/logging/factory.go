package logging

import (
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"time"
)

//
// Builder.
type Builder interface {
	New() logr.Logger
	V(int, logr.Logger) logr.Logger
}

//
// Zap builder factory.
type ZapBuilder struct {
}

//
// Build new logger.
func (b *ZapBuilder) New() (l logr.Logger) {
	var encoder zapcore.Encoder
	var options []zap.Option
	sinker := zapcore.AddSync(os.Stderr)
	if Settings.Development {
		cfg := zap.NewDevelopmentEncoderConfig()
		encoder = zapcore.NewConsoleEncoder(cfg)
		options = append(
			options,
			zap.Development(),
			zap.AddStacktrace(zap.ErrorLevel))
	} else {
		cfg := zap.NewProductionEncoderConfig()
		encoder = zapcore.NewJSONEncoder(cfg)
		options = append(
			options,
			zap.AddStacktrace(zap.WarnLevel),
			zap.WrapCore(
				func(core zapcore.Core) zapcore.Core {
					return zapcore.NewSampler(
						core,
						time.Second,
						100,
						100)
				}))
	}
	level := zap.NewAtomicLevelAt(zap.DebugLevel)
	options = append(
		options,
		zap.AddCallerSkip(1),
		zap.ErrorOutput(sinker))
	log := zap.New(
		zapcore.NewCore(
			encoder,
			sinker,
			level))
	log = log.WithOptions(options...)
	l = zapr.NewLogger(log)

	return
}

//
// Debug logger.
func (b *ZapBuilder) V(level int, in logr.Logger) (l logr.Logger) {
	if Settings.atDebug(level) {
		l = in.V(1)
	} else {
		l = in.V(0)
	}

	return
}
