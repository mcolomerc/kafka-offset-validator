# Kafka Offset Validator

## Overview

The Kafka Offset Validator is a web service that connects to two different Kafka clusters to validate consumer group offset information. It helps ensure that the offsets in both clusters are consistent and provides a mechanism to report any discrepancies.

## Setup Instructions

1. **Clone the repository:**

```sh
   git clone <repository-url>
   cd kafka-offset-validator
```

2. **Install dependencies:**

```sh
go mod tidy
```

3. **Configure Kafka Clusters:**

Update the configuration in `.env` to include the connection parameters for both Kafka clusters. You can load these from environment variables or configuration files.

4. **Run the application:**

Run the server using the following command:

```sh
go run main.go
```

## Usage

Once the service is running, you can trigger the validation process by sending a request to the appropriate endpoint  

`http://localhost:8080/offsets/validate?group_id=<consumer-group-id>&topics=<topics-name>`

## Configuration

The configuration for connecting to the Kafka clusters: `.env` file can be used to set environment variables for configuration. Ensure that you have the necessary Kafka credentials and settings defined.

Environment variables can be set in a `.env` file or directly in your shell. Below is an example of how to set up the environment variables for both source and destination Kafka clusters:

```dotenv
# Source Kafka Configuration
SOURCE_KAFKA_BROKERS="your-source-broker:9092" 
SOURCE_KAFKA_SASL_MECHANISMS="PLAIN"
SOURCE_KAFKA_SECURITY_PROTOCOL="SASL_SSL"
SOURCE_KAFKA_SASL_USERNAME="your-source-username"
SOURCE_KAFKA_SASL_PASSWORD="your-source-password"
SOURCE_KAFKA_CLIENT_ID="go-offset-validator" 

# Destination Kafka Configuration
DESTINATION_KAFKA_BROKERS="your-destination-broker:9092" 
DESTINATION_KAFKA_SASL_MECHANISMS="PLAIN"
DESTINATION_KAFKA_SECURITY_PROTOCOL="SASL_SSL"
DESTINATION_KAFKA_SASL_USERNAME="your-destination-username"
DESTINATION_KAFKA_SASL_PASSWORD="your-destination-password"
DESTINATION_KAFKA_CLIENT_ID="go-offset-validator" 
```

See `.env.template` for an example of how to set up the environment variables.

## Troubleshooting Common Issues

```json
{"error":"failed to list offsets: failed to list consumer group offsets: LISTCONSUMERGROUPOFFSETS worker coordinator request failed: Broker: Group authorization failed"}
```

* Required ACLs - Consumer Group Access

You must grant access to the consumer group that will be listing the offsets. Specifically, permissions needed include:
Describe for the consumer group metadata.

Offsets Topic Access:

The ACLs required to read offset information may also include the following for the `__consumer_offsets` topic:
Describe permission on the `__consumer_offsets` topic to view group offsets.

Admin Permissions:
Depending on your specific scenario, you might also require admin permissions like All or Create for general operations on the offsets

* If you are using a Confluent Cloud Cluster, just grant the service account as Cluster Admin (`CloudClusterAdmin`) for the service account.


## Example

```sh
curl http://localhost:8080/offsets/validate\?group_id\=go-example-consumer-group\&topics\=topic_11
```
 
Sample Response:

```json
{
    "group_id": "go-example-consumer-group",
    "matches": false,
    "source_offsets": {
        "go-example-consumer-group": [
            {
                "topic": "topic_11",
                "partition": 0,
                "offset": 2,
                "cluster": "source"
            },
            {
   ... 
    "destination_offsets": {
        "go-example-consumer-group": [
            {
                "topic": "topic_11",
                "partition": 0,
                "offset": -1001,
                "cluster": "destination"
            },
            {
                "topic": "topic_11",
                "partition": 1,
                "offset": -1001,
                "cluster": "destination"
            },
```

Offset = -1001 indicates that the consumer group has not consumed any messages from the topic partition.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.
 
 
