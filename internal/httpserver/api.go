package httpserver

import (
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/pkg/api"
)

type apiHandler struct {
	logger *slog.Logger
	store  *store.Store
	redis  *redis.Client
}

func NewAPIRouter(logger *slog.Logger, data *store.Store, cache *redis.Client, apiKey string, registry prometheus.Gatherer) *gin.Engine {
	router := NewRouter("api", logger, registry)
	handler := &apiHandler{logger: logger, store: data, redis: cache}
	v1 := router.Group("/v1", requireAPIKey(apiKey))
	v1.POST("/jobs", handler.createJob)
	v1.POST("/jobs/batch", handler.createBatch)
	v1.GET("/jobs/:id", handler.getJob)
	v1.GET("/jobs/:id/runs", handler.listJobRuns)
	v1.POST("/jobs/:id/run-now", handler.runNow)
	v1.POST("/jobs/:id/pause", func(c *gin.Context) { handler.setJobPaused(c, true) })
	v1.POST("/jobs/:id/resume", func(c *gin.Context) { handler.setJobPaused(c, false) })
	v1.GET("/runs/:id", handler.getRun)
	v1.POST("/runs/:id/cancel", handler.cancelRun)
	return router
}

func requireAPIKey(apiKey string) gin.HandlerFunc {
	return func(c *gin.Context) {
		const prefix = "Bearer "
		authorization := c.GetHeader("Authorization")
		provided := ""
		if strings.HasPrefix(authorization, prefix) {
			provided = strings.TrimSpace(strings.TrimPrefix(authorization, prefix))
		}
		if apiKey == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(apiKey)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, api.ErrorResponse{Error: "unauthorized"})
			return
		}
		c.Next()
	}
}

func (h *apiHandler) createJob(c *gin.Context) {
	var request api.CreateJobRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, api.ErrorResponse{Error: "invalid JSON body"})
		return
	}
	key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if key != "" {
		if id, err := h.redis.Get(c.Request.Context(), "idem:"+key).Result(); err == nil {
			if job, err := h.store.GetJob(c.Request.Context(), id); err == nil && job.IdempotencyKey == key {
				c.JSON(http.StatusOK, job)
				return
			}
		} else if !errors.Is(err, redis.Nil) {
			h.store.Metrics.RedisErrors.Inc()
			h.logger.Warn("could not read idempotency key", "error", err)
		}
	}

	job, err := h.store.CreateJob(c.Request.Context(), store.Job{
		Name:           request.Name,
		Type:           request.Type,
		Payload:        request.Payload,
		CronExpr:       request.CronExpr,
		Timezone:       request.Timezone,
		NextFireAt:     request.NextFireAt,
		MisfirePolicy:  request.MisfirePolicy,
		Paused:         request.Paused,
		MaxAttempts:    request.MaxAttempts,
		BackoffBaseMS:  request.BackoffBaseMS,
		BackoffMaxMS:   request.BackoffMaxMS,
		TimeoutMS:      request.TimeoutMS,
		IdempotencyKey: key,
	}, request.ScheduledFor)
	if err != nil {
		h.writeStoreError(c, err)
		return
	}
	if key != "" {
		if err := h.redis.SetNX(c.Request.Context(), "idem:"+key, job.ID, 24*time.Hour).Err(); err != nil {
			h.store.Metrics.RedisErrors.Inc()
			h.logger.Warn("could not cache idempotency key", "error", err)
		}
	}
	c.JSON(http.StatusCreated, job)
}

func (h *apiHandler) createBatch(c *gin.Context) {
	var request api.CreateBatchRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, api.ErrorResponse{Error: "invalid JSON body"})
		return
	}
	jobs, replay, err := h.store.CreateJobs(c.Request.Context(), request.Jobs,
		strings.TrimSpace(c.GetHeader("Idempotency-Key")))
	if err != nil {
		h.writeStoreError(c, err)
		return
	}
	status := http.StatusCreated
	if replay {
		status = http.StatusOK
	}
	c.JSON(status, jobs)
}

func (h *apiHandler) getJob(c *gin.Context) {
	if !validPathID(c) {
		return
	}
	job, err := h.store.GetJob(c.Request.Context(), c.Param("id"))
	if err != nil {
		h.writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusOK, job)
}

func (h *apiHandler) listJobRuns(c *gin.Context) {
	if !validPathID(c) {
		return
	}
	limit, offset, ok := runListBounds(c)
	if !ok {
		return
	}
	if _, err := h.store.GetJob(c.Request.Context(), c.Param("id")); err != nil {
		h.writeStoreError(c, err)
		return
	}
	runs, err := h.store.ListJobRuns(c.Request.Context(), c.Param("id"), limit, offset)
	if err != nil {
		h.writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusOK, runs)
}

func (h *apiHandler) runNow(c *gin.Context) {
	if !validPathID(c) {
		return
	}
	run, err := h.store.RunNow(c.Request.Context(), c.Param("id"))
	if err != nil {
		h.writeStoreError(c, err)
		return
	}
	h.logger.Debug("created immediate run", "run_id", run.ID, "attempt", run.Attempt)
	c.JSON(http.StatusCreated, run)
}

func (h *apiHandler) setJobPaused(c *gin.Context, paused bool) {
	if !validPathID(c) {
		return
	}
	job, err := h.store.SetJobPaused(c.Request.Context(), c.Param("id"), paused)
	if err != nil {
		h.writeStoreError(c, err)
		return
	}
	h.logger.Info("job pause state updated", "job_id", job.ID, "paused", job.Paused)
	c.JSON(http.StatusOK, job)
}

func (h *apiHandler) getRun(c *gin.Context) {
	if !validPathID(c) {
		return
	}
	run, err := h.store.GetRun(c.Request.Context(), c.Param("id"))
	if err != nil {
		h.writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusOK, run)
}

func (h *apiHandler) cancelRun(c *gin.Context) {
	if !validPathID(c) {
		return
	}
	run, err := h.store.CancelRun(c.Request.Context(), c.Param("id"))
	if err != nil {
		h.writeStoreError(c, err)
		return
	}
	h.logger.Info("run cancellation accepted", "run_id", run.ID, "attempt", run.Attempt, "status", run.Status)
	c.JSON(http.StatusOK, run)
}

func validPathID(c *gin.Context) bool {
	if _, err := uuid.Parse(c.Param("id")); err != nil {
		c.JSON(http.StatusBadRequest, api.ErrorResponse{Error: "invalid id"})
		return false
	}
	return true
}

func runListBounds(c *gin.Context) (int, int, bool) {
	limit, offset := 50, 0
	var err error
	if value := c.Query("limit"); value != "" {
		limit, err = strconv.Atoi(value)
	}
	if err == nil && c.Query("offset") != "" {
		offset, err = strconv.Atoi(c.Query("offset"))
	}
	if err != nil || limit < 1 || limit > 100 || offset < 0 {
		c.JSON(http.StatusBadRequest, api.ErrorResponse{Error: "limit must be 1-100 and offset must be non-negative"})
		return 0, 0, false
	}
	return limit, offset, true
}

func (h *apiHandler) writeStoreError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		c.JSON(http.StatusNotFound, api.ErrorResponse{Error: "not found"})
	case errors.Is(err, store.ErrInvalidJob):
		c.JSON(http.StatusBadRequest, api.ErrorResponse{Error: "invalid job"})
	case errors.Is(err, store.ErrIdempotencyConflict):
		c.JSON(http.StatusConflict, api.ErrorResponse{Error: "idempotency key reused with a different batch"})
	case errors.Is(err, store.ErrInvalidTransition):
		c.JSON(http.StatusConflict, api.ErrorResponse{Error: "invalid state transition"})
	default:
		h.logger.Error("API storage operation failed", "error", err)
		c.JSON(http.StatusInternalServerError, api.ErrorResponse{Error: "internal server error"})
	}
}
