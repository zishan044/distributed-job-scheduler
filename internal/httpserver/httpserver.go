package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(service string, logger *slog.Logger, registry prometheus.Gatherer) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.RecoveryWithWriter(logWriter{logger: logger}), requestLogger(logger))
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"service": service, "status": "ok"})
	})
	router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(registry, promhttp.HandlerOpts{})))
	return router
}

func Serve(addr string, handler http.Handler, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return ServeContext(ctx, addr, handler, logger)
}

// ServeContext serves until ctx is cancelled, then closes the listener and
// drains requests for up to ten seconds. It joins the serving goroutine.
func ServeContext(ctx context.Context, addr string, handler http.Handler, logger *slog.Logger) error {
	if ctx.Err() != nil {
		return nil
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		logger.Info("shutting down HTTP server")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := server.Shutdown(shutdownCtx)
	if err != nil {
		// Shutdown does not close active connections when its deadline expires.
		err = errors.Join(err, server.Close())
	}
	<-serveErr
	return err
}

func requestLogger(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()

		level := slog.LevelInfo
		if c.Writer.Status() >= http.StatusInternalServerError {
			level = slog.LevelError
		}
		logger.Log(c.Request.Context(), level, "http request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration", time.Since(started),
		)
	}
}

type logWriter struct {
	logger *slog.Logger
}

func (w logWriter) Write(p []byte) (int, error) {
	w.logger.Error("recovered HTTP panic", "details", string(p))
	return len(p), nil
}
