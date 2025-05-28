package main

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"mcolomerc/kafka-offset-validator/handler"
	"mcolomerc/kafka-offset-validator/kafkaclient"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/joho/godotenv"
)

type Config struct {
	SourceKafka      KafkaConfig `json:"source_kafka"`
	DestinationKafka KafkaConfig `json:"destination_kafka"`
}

type KafkaConfig struct {
	Brokers          []string `json:"brokers"`
	GroupID          string   `json:"group_id"`
	SASLMechanisms   string   `json:"sasl_mechanisms"`
	SecurityProtocol string   `json:"security_protocol"`
	SASLUsername     string   `json:"sasl_username"`
	SASLPassword     string   `json:"sasl_password"`
	ClientID         string   `json:"client_id"`
	SessionTimeoutMs int      `json:"session_timeout_ms"`
	AutoOffsetReset  string   `json:"auto_offset_reset"`
}

var (
	instance *Config
	once     sync.Once
)

func LoadConfig() (*Config, error) {
	var err error
	once.Do(func() {
		instance, err = loadConfigFromEnv()
	})
	return instance, err
}

func loadConfigFromEnv() (*Config, error) {
	sourceKafka := KafkaConfig{
		Brokers:          getEnvAsSlice("SOURCE_KAFKA_BROKERS"),
		GroupID:          os.Getenv("SOURCE_KAFKA_GROUP_ID"),
		SASLMechanisms:   getEnvOrDefault("SOURCE_KAFKA_SASL_MECHANISMS", "PLAIN"),
		SecurityProtocol: getEnvOrDefault("SOURCE_KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
		SASLUsername:     os.Getenv("SOURCE_KAFKA_SASL_USERNAME"),
		SASLPassword:     os.Getenv("SOURCE_KAFKA_SASL_PASSWORD"),
		ClientID:         getEnvOrDefault("SOURCE_KAFKA_CLIENT_ID", "go-offset-validator"),
		SessionTimeoutMs: getEnvAsInt("SOURCE_KAFKA_SESSION_TIMEOUT_MS", 6000),
		AutoOffsetReset:  getEnvOrDefault("SOURCE_KAFKA_AUTO_OFFSET_RESET", "earliest"),
	}

	destinationKafka := KafkaConfig{
		Brokers:          getEnvAsSlice("DESTINATION_KAFKA_BROKERS"),
		GroupID:          os.Getenv("DESTINATION_KAFKA_GROUP_ID"),
		SASLMechanisms:   getEnvOrDefault("DESTINATION_KAFKA_SASL_MECHANISMS", "PLAIN"),
		SecurityProtocol: getEnvOrDefault("DESTINATION_KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
		SASLUsername:     os.Getenv("DESTINATION_KAFKA_SASL_USERNAME"),
		SASLPassword:     os.Getenv("DESTINATION_KAFKA_SASL_PASSWORD"),
		ClientID:         getEnvOrDefault("DESTINATION_KAFKA_CLIENT_ID", "go-offset-validator"),
		SessionTimeoutMs: getEnvAsInt("DESTINATION_KAFKA_SESSION_TIMEOUT_MS", 6000),
		AutoOffsetReset:  getEnvOrDefault("DESTINATION_KAFKA_AUTO_OFFSET_RESET", "earliest"),
	}

	return &Config{
		SourceKafka:      sourceKafka,
		DestinationKafka: destinationKafka,
	}, nil
}

func getKafkaConfigMap(config KafkaConfig) map[string]interface{} {
	return map[string]interface{}{
		"bootstrap.servers":  strings.Join(config.Brokers, ","),
		"group.id":           config.GroupID,
		"sasl.mechanisms":    config.SASLMechanisms,
		"security.protocol":  config.SecurityProtocol,
		"sasl.username":      config.SASLUsername,
		"sasl.password":      config.SASLPassword,
		"client.id":          config.ClientID,
		"session.timeout.ms": config.SessionTimeoutMs,
		"auto.offset.reset":  config.AutoOffsetReset,
	}
}

func getEnvAsSlice(key string) []string {
	value := os.Getenv(key)
	if value == "" {
		return []string{}
	}
	return splitString(value, ",")
}

func splitString(s string, sep string) []string {
	return strings.Split(s, sep)
}

func getEnvOrDefault(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}

func getEnvAsInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	var i int
	_, err := fmt.Sscanf(val, "%d", &i)
	if err != nil {
		return defaultVal
	}
	return i
}

// Helper function to convert map[string]interface{} to *kafka.ConfigMap
func mapToKafkaConfigMap(m map[string]interface{}) *kafka.ConfigMap {
	cfg := &kafka.ConfigMap{}
	for k, v := range m {
		cfg.SetKey(k, v)
	}
	return cfg
}

func init() {
	_ = godotenv.Load() // Loads .env file into environment variables
}

func main() {
	config, err := LoadConfig()
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
	}
	// Build a kafka ConfigMap from config.SourceKafka
	// and config.DestinationKafka

	// Convert map[string]interface{} to *kafka.ConfigMap
	sourceKafkaConfigMap := mapToKafkaConfigMap(getKafkaConfigMap(config.SourceKafka))
	destKafkaConfigMap := mapToKafkaConfigMap(getKafkaConfigMap(config.DestinationKafka))

	// Kafka Client initialization would go here
	kClient, err := kafkaclient.NewKafkaClient(
		sourceKafkaConfigMap,
		destKafkaConfigMap)
	if err != nil {
		fmt.Printf("Error creating Kafka client: %v\n", err)
		os.Exit(1)
	}
	// Kafka Handler initialization would go here
	kafkaHandler := handler.NewHandler(kClient)

	httpPort := 8080
	fmt.Printf("HTTP server will run on port: %d\n", httpPort)
	// Initialize the Gin router and handlers here
	router := handler.NewRouter(kafkaHandler) // You may need to implement NewRouter in your handler package
	// Start the HTTP server
	fmt.Printf("Starting server on :%d\n", httpPort)
	if err := router.Run(fmt.Sprintf(":%d", httpPort)); err != nil {
		fmt.Printf("Could not start server: %s\n", err)
		os.Exit(1)
	}
}
