package handler

import (
	"mcolomerc/kafka-offset-validator/kafkaclient"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	kafkaClient *kafkaclient.KafkaClient
}

// NewRouter creates and returns a Gin engine with routes set up for the handler.
func NewRouter(h *Handler) *gin.Engine {
	router := gin.Default()
	// Example route, replace/add as needed:
	router.GET("/health", h.HealthCheck)
	// Add more routes here as needed
	router.GET("/offsets/validate", h.ValidateOffsets)
	return router
}

func NewHandler(kafkaClient *kafkaclient.KafkaClient) *Handler {
	return &Handler{kafkaClient: kafkaClient}
}

func (h *Handler) ValidateOffsets(c *gin.Context) {
	// Validate query parameters
	if c.Query("group_id") == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "group_id is required"})
		return
	}
	if len(c.QueryArray("topics")) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one topic is required"})
		return
	}
	results, err := h.kafkaClient.ValidateConsumerGroupOffsets(c.Request.Context(),
		c.Query("group_id"),
		c.QueryArray("topics"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, results)
}

func (h *Handler) SetupRoutes(router *gin.Engine) {
	router.POST("/offsets", h.ValidateOffsets)
}

func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
}
