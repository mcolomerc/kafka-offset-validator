package handler

import (
	"fmt"
	"mcolomerc/kafka-offset-validator/kafkaclient"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
	router.GET("/offsets/lookback", h.GetOffsetsAtTimestamp)
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

// Add a new handler to get the offsets for a topic in a given timestamp
func (h *Handler) GetOffsetsAtTimestamp(c *gin.Context) {
	// Validate query parameters
	topic := c.Query("topic")
	if topic == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "topic is required"})
		return
	}

	cluster := c.DefaultQuery("cluster", "source") // default to source
	if cluster != "source" && cluster != "destination" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cluster must be 'source' (default) or 'destination'"})
		return
	}

	timestampStr := c.Query("timestamp")
	lookback := c.Query("lookback")

	// Validate that timestamp and lookback are not used together
	if timestampStr != "" && lookback != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot use both 'timestamp' and 'lookback' parameters together"})
		return
	}

	var timestamp int64
	var err error

	// Check if timestamp is provided directly
	if timestampStr != "" {
		timestamp, err = strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid timestamp format"})
			return
		}
	} else if lookback != "" {
		// Parse lookback and calculate timestamp
		timestamp, err = h.parseLookbackToTimestamp(lookback)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid lookback format: " + err.Error()})
			return
		}
	} else {
		// Default to 0 (latest) if neither timestamp nor lookback is provided
		timestamp = 0
	}

	// Select the appropriate admin client
	adminClient, err := h.kafkaClient.GetClusterAdmin(cluster)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get admin client: " + err.Error()})
		return
	}

	results, err := h.kafkaClient.GetOffsetsAtTimestamp(c.Request.Context(), adminClient, topic, timestamp)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Check if topic was found (no results returned)
	if len(results) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "topic not found"})
		return
	}

	// Transform results to Terraform-compatible format
	terraformResponse := h.transformToTerraformFormat(topic, results)

	c.JSON(http.StatusOK, terraformResponse)
}

func (h *Handler) transformToTerraformFormat(topic string, results []kafka.TopicPartition) gin.H {
	var offsets []gin.H

	for _, tp := range results {
		offset := gin.H{
			"partition": gin.H{
				"kafka_partition": fmt.Sprintf("%d", tp.Partition),
				"kafka_topic":     topic,
			},
			"offset": gin.H{
				"kafka_offset": fmt.Sprintf("%d", tp.Offset),
			},
		}
		offsets = append(offsets, offset)
	}

	return gin.H{
		"offsets": offsets,
	}
}

func (h *Handler) parseLookbackToTimestamp(lookback string) (int64, error) {
	lookback = strings.ToLower(strings.TrimSpace(lookback))

	var multiplier time.Duration
	var valueStr string

	if strings.HasSuffix(lookback, "s") {
		multiplier = time.Second
		valueStr = strings.TrimSuffix(lookback, "s")
	} else if strings.HasSuffix(lookback, "m") {
		multiplier = time.Minute
		valueStr = strings.TrimSuffix(lookback, "m")
	} else if strings.HasSuffix(lookback, "h") {
		multiplier = time.Hour
		valueStr = strings.TrimSuffix(lookback, "h")
	} else if strings.HasSuffix(lookback, "d") {
		multiplier = 24 * time.Hour
		valueStr = strings.TrimSuffix(lookback, "d")
	} else {
		return 0, fmt.Errorf("lookback must end with 's' (seconds), 'm' (minutes), 'h' (hours), or 'd' (days)")
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return 0, fmt.Errorf("invalid lookback value: %s", valueStr)
	}

	pastTime := time.Now().Add(-time.Duration(value) * multiplier)
	return pastTime.UnixMilli(), nil
}

func (h *Handler) HealthCheck(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
}
