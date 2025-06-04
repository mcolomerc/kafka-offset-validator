# Kafka Offset Validator

## Overview

The Kafka Offset Validator is a web service that connects to two different Kafka clusters to validate consumer group offset information. It helps ensure that the offsets in both clusters are consistent and provides a mechanism to report any discrepancies.

It allows to query offsets for a specific topic at a historical timestamp, which is useful for validating offsets at a specific point in time.

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

5. **Build the application:**

To build the application, you can use the following command:

```sh
go build -o kafka-offset-validator main.go
```

6. **Run the built application:**
   
```sh
./kafka-offset-validator
```

## Usage

### Configuration

The configuration for connecting to the Kafka clusters: `.env` file can be used to set environment variables for configuration. Ensure that you have the necessary Kafka credentials and settings defined.

Environment variables can be set in a `.env` file or directly in your shell. Below is an example of how to set up the environment variables for both source and destination Kafka clusters:

```dotenv
# Source Kafka Configuration
SOURCE_KAFKA_BROKERS="your-source-broker:9092"
SOURCE_KAFKA_GROUP_ID="your-source-group-id"
SOURCE_KAFKA_SASL_MECHANISMS="PLAIN"
SOURCE_KAFKA_SECURITY_PROTOCOL="SASL_SSL"
SOURCE_KAFKA_SASL_USERNAME="your-source-username"
SOURCE_KAFKA_SASL_PASSWORD="your-source-password"
SOURCE_KAFKA_CLIENT_ID="go-offset-validator"
SOURCE_KAFKA_SESSION_TIMEOUT_MS="6000"
SOURCE_KAFKA_AUTO_OFFSET_RESET="earliest"

# Destination Kafka Configuration
DESTINATION_KAFKA_BROKERS="your-destination-broker:9092"
DESTINATION_KAFKA_GROUP_ID="your-destination-group-id"
DESTINATION_KAFKA_SASL_MECHANISMS="PLAIN"
DESTINATION_KAFKA_SECURITY_PROTOCOL="SASL_SSL"
DESTINATION_KAFKA_SASL_USERNAME="your-destination-username"
DESTINATION_KAFKA_SASL_PASSWORD="your-destination-password"
DESTINATION_KAFKA_CLIENT_ID="go-offset-validator"
DESTINATION_KAFKA_SESSION_TIMEOUT_MS="6000"
DESTINATION_KAFKA_AUTO_OFFSET_RESET="earliest"
```

See `.env.template` for an example of how to set up the environment variables.

```sh
cp .env.template .env
```

### Validate Consumer Group Offsets

Once the service is running, you can trigger the validation process by sending a request to the appropriate endpoint  

`GET /offsets/validate?group_id=<consumer-group-id>&topics=<topics-name>`

Example:

```bash
curl "http://localhost:8080/offsets/validate?group_id=go-example-consumer-group&topics=topic_11"
```

Response:

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
                "topic": "topic_11",
                "partition": 1,
                "offset": 3,
                "cluster": "source"
            }
        ]
    },
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
            }
        ]
    }
}
```

Offset = -1001 indicates that the consumer group has not consumed any messages from the topic partition.

### Get Topic Offsets at Historical Timestamp

Get the offsets for a topic at a specific point in the past using a lookback duration:

`GET /offsets/lookback?topic=<topic-name>&lookback=<duration>&cluster=<cluster>`

**Parameters:**

- `topic` (required): The topic name
  
- `timestamp` (optional): Unix timestamp in milliseconds. Use specific values:
  - Any positive number: Get offsets at that specific timestamp
  - `-1`: Get latest offsets (end of topic)
  - `-2`: Get earliest offsets (beginning of topic)
  - If not provided: Defaults to latest offsets
  
- `lookback` (optional): Duration to go back from now (alternative to timestamp), timestamp and lookback cannot be used together.
  - `s` for seconds (e.g., `30s`)
  - `m` for minutes (e.g., `15m`)
  - `h` for hours (e.g., `2h`)
  - `d` for days (e.g., `1d`)

When using `lookback`, it calculates the timestamp based on the current time minus the specified duration.
  
- `cluster` (optional): Which cluster to query (`source` or `destination`, defaults to `source`)

**Examples:**

```bash
# Get latest offsets (default behavior)
curl "http://localhost:8080/offsets/lookback?topic=orders"

# Get offsets at specific timestamp
curl "http://localhost:8080/offsets/lookback?topic=orders&timestamp=1735689600000"

# Get earliest offsets
curl "http://localhost:8080/offsets/lookback?topic=orders&timestamp=-2"

# Get latest offsets explicitly
curl "http://localhost:8080/offsets/lookback?topic=orders&timestamp=-1"

# Get offsets from 15 minutes ago
curl "http://localhost:8080/offsets/lookback?topic=orders&lookback=15m"

# Get offsets from 2 hours ago on destination cluster
curl "http://localhost:8080/offsets/lookback?topic=orders&lookback=2h&cluster=destination"

# Get offsets from 30 seconds ago
curl "http://localhost:8080/offsets/lookback?topic=orders&lookback=30s"
```

**Response Format:**

The response is formatted for Terraform consumption:

```json
{
  "offsets": [
    {
      "partition": {
        "kafka_partition": "0",
        "kafka_topic": "topic_name"
      },
      "offset": {
        "kafka_offset": "100"
      }
    },
    {
      "partition": {
        "kafka_partition": "1", 
        "kafka_topic": "topic_name"
      },
      "offset": {
        "kafka_offset": "200"
      }
    }
  ]
}
```

**Note:** You can use either `timestamp` OR `lookback`, not both. If neither is provided, it defaults to latest offsets.

**Using the response with Terraform:**

The response from the `/offsets/lookback` endpoint is formatted to be compatible with Terraform's [Confluent Connector](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/confluent_connector) to create a connector with the offsets.

You can use the output directly in your Terraform configuration, Terraform variables or scripts.

Having the following Terraform variable defined:

```terraform
variable "connectors_offsets" {
  type = list(object({
    name = string
    offsets = list(object({
      partition = object({
        kafka_partition = string
        kafka_topic     = string
      })
      offset = object({
        kafka_offset = string
      })
    }))
  }))
}
```

To use the response with Terraform, you can format it as follows:

Use `curl "http://localhost:8080/offsets/lookback?topic=orders&lookback=4h" | jq '{"name": "orders-connector"} + .'`

```json
{
  "name": "orders-connector",
  "offsets": [
    {
      "partition": {
        "kafka_partition": "0",
        "kafka_topic": "orders"
      },
      "offset": {
        "kafka_offset": "100"
      }
    },
    {
      "partition": {
        "kafka_partition": "1",
        "kafka_topic": "orders"
      },
      "offset": {
        "kafka_offset": "200"
      }
    }
  ]
}
```

## Troubleshooting

**Permissions:**

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

**Offset Always Returns 0:**

If the `/offsets/lookback` endpoint always returns `"kafka_offset": "0"`, this usually means:

1. **No messages at that timestamp**: No messages were produced to the topic at the exact time you're querying
2. **Topic is new or has low activity**: The topic doesn't have messages going back that far
3. **Messages have been cleaned up**: Topic retention policies may have removed older messages

**To verify:**

```bash
# Check if there are recent messages (last hour)
curl "http://localhost:8080/offsets/lookback?topic=orders&lookback=1h"

```

**Solutions:**

- Try shorter lookback periods (e.g., `30s`, `1m`)
  
- Verify the topic has messages in the timeframe you're querying
  
- Check topic retention settings
  
- Use a topic with known recent activity for testing


## How Timestamp-Based Offset Querying Works

Kafka stores messages with timestamps, and the offset validator can query for the **earliest offset** that contains a message with a timestamp greater than or equal to the specified timestamp.

1. **Message Timestamps**: Each Kafka message has a timestamp (either producer-set or broker-set)
2. **Offset Resolution**: Kafka returns the **earliest offset** where `message.timestamp >= query.timestamp`
3. **Partition-Level Queries**: Timestamps are queried per partition, not per topic
4. **Time-Based Seeking**: This is useful for replaying messages from a specific point in time

**Timestamp Precision:**

- Kafka uses **millisecond precision** (Unix timestamp in milliseconds)
- The API returns the first message at or after the requested timestamp
- If no messages exist at/after that timestamp, returns `-1`

**Message Retention:**

- Only works within the topic's **retention period**
- Older messages may have been deleted by Kafka's cleanup policies
- Check topic retention settings if historical queries fail

**Timestamp Types:**

Kafka supports two timestamp types (configured per topic):

- `CreateTime`: Timestamp when producer created the message (default)
  
- `LogAppendTime`: Timestamp when broker received the message

**Producer Behavior:**

- Producers can set custom timestamps (including future timestamps)
- Clock skew between producers can affect timestamp ordering
- Out-of-order messages may have non-monotonic timestamps

**Performance Considerations:**

- Timestamp queries require index lookups across partitions
- Queries on topics with many partitions may take longer
- Consider using partition-specific queries for better performance

**Common Use Cases:**

1. **Disaster Recovery**: Find offsets to resume processing from a specific time
2. **Data Reprocessing**: Replay messages from when an issue occurred
3. **Connector Initialization**: Set initial offsets for new connectors
4. **Compliance**: Query data for specific time windows

**Limitations:**

- **No Consumer Group History**: Cannot get where a consumer group was at a historical timestamp
- **Retention Dependent**: Limited by topic retention policies
- **Timestamp Accuracy**: Depends on producer clock synchronization
- **Partition Distribution**: Results vary if topic partition count changed over time
  
## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.
