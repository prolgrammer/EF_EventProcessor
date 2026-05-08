package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New returns a production-grade structured zap logger that writes JSON to
// stdout. The service name is injected into every log entry.
func New(service string) (*zap.Logger, error) {
	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "ts"
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.MessageKey = "event"

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg),
		zapcore.AddSync(os.Stdout),
		zapcore.InfoLevel,
	)

	hostname, _ := os.Hostname()

	log := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel)).
		With(
			zap.String("service", service),
			zap.String("instance", hostname),
		)

	return log, nil
}

// Must is a helper that panics if New returns an error.
func Must(service string) *zap.Logger {
	l, err := New(service)
	if err != nil {
		panic(fmt.Sprintf("logger.Must: %v", err))
	}
	return l
}
