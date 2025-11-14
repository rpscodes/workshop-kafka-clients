# Kafka Java Tutorial Application

A simple, beginner-friendly Java application demonstrating Kafka Producer and Consumer basics with configurable parameters to show behavior differences.

## Prerequisites

- Java 17 or higher
- Maven 3.6 or higher
- Access to a Kafka cluster (configured for OpenShift: `kafka-kafka-bootstrap.kafka-user1.svc.cluster.local:9092`)

## Project Structure

```
workshop-kafka-clients/
├── pom.xml                                    # Maven project configuration
├── README.md                                  # This file
└── src/main/java/com/kafka/tutorial/
    ├── KafkaTutorialApp.java                 # Main menu application
    ├── KafkaConfig.java                      # Configuration constants
    ├── SimpleKafkaProducer.java              # Producer implementation
    └── SimpleKafkaConsumer.java              # Consumer implementation
```

## Building the Project

```bash
mvn clean package
```

This will create an executable JAR file in the `target` directory:
- `kafka-java-tutorial-1.0.0-jar-with-dependencies.jar`

## Running the Application

### Option 1: Use the run script (Easiest)

```bash
./run.sh
```

This script will automatically build and run the application.

### Option 2: Run with Maven

```bash
mvn exec:java -Dexec.mainClass="com.kafka.tutorial.KafkaTutorialApp"
```

### Option 3: Run the JAR file

First, build the project:
```bash
mvn clean package
```

Then run the JAR:
```bash
java -jar target/kafka-java-tutorial-1.0.0-jar-with-dependencies.jar
```

## Usage

The application provides an interactive menu:

1. **Run Producer** - Send sample messages to Kafka
2. **Run Consumer** - Consume messages from Kafka
3. **Exit** - Exit the application

### Running the Producer

When you select "Run Producer", you'll be prompted to configure:

- **Batch Size**: Size in bytes for batching messages
  - Default: 16384 bytes (16 KB)
  - Try: 1024 (small) vs 32768 (large) to see batching behavior
- **Linger.ms**: Time to wait before sending a batch
  - Default: 0 ms (send immediately)
  - Try: 0 vs 100 to see batching behavior

The producer will automatically send 10 sample messages to the topic `tutorial-topic`.

### Running the Consumer

When you select "Run Consumer", you'll be prompted to configure:

- **Auto Offset Reset**: Where to start reading messages
  - `earliest`: Read ALL messages from the beginning of the topic
  - `latest`: Only read NEW messages sent after the consumer starts
  - Default: `latest`
- **Max Messages**: Number of messages to consume (0 = unlimited)

## Configuration Parameters Explained

### Producer Configuration Parameters

#### Required Parameters

1. **bootstrap.servers**
   - **Purpose**: Tells the producer where to find the Kafka cluster
   - **Value**: `kafka-kafka-bootstrap.kafka-user1.svc.cluster.local:9092`
   - **Why it matters**: Without this, the producer can't connect to Kafka

2. **key.serializer** / **value.serializer**
   - **Purpose**: Convert Java objects to bytes for network transmission
   - **Value**: `org.apache.kafka.common.serialization.StringSerializer`
   - **Why it matters**: Kafka stores data as bytes, so objects must be serialized

#### Important Optional Parameters

3. **acks**
   - **Purpose**: Controls producer reliability
   - **Options**:
     - `"all"`: Wait for all in-sync replicas to acknowledge (most reliable, slower)
     - `"1"`: Wait for leader to acknowledge (balanced)
     - `"0"`: Don't wait for acknowledgment (fastest, least reliable)
   - **Default in this app**: `"all"`

4. **batch.size** (Demo Parameter)
   - **Purpose**: Maximum size of a batch in bytes before sending
   - **Effect**:
     - **Larger values** (e.g., 32768): Better throughput, more messages batched together
     - **Smaller values** (e.g., 1024): Lower latency, messages sent more frequently
   - **Default**: 16384 bytes (16 KB)
   - **Demo**: Try changing from 16384 to 1024 to see messages sent more frequently

5. **linger.ms** (Demo Parameter)
   - **Purpose**: Time to wait before sending a batch (even if batch isn't full)
   - **Effect**:
     - **0 ms**: Send immediately (lower latency, less batching)
     - **>0 ms** (e.g., 100): Wait to accumulate more messages (better throughput, more batching)
   - **Default**: 0 ms
   - **Demo**: Try changing from 0 to 100 to see batching behavior

#### Additional Parameters (Commented in Code)

- **retries**: Number of retries on failure
- **max.in.flight.requests.per.connection**: Parallelism for sending messages
- **compression.type**: Compression algorithm (e.g., "snappy", "gzip")
- **buffer.memory**: Total memory for buffering unsent messages

### Consumer Configuration Parameters

#### Required Parameters

1. **bootstrap.servers**
   - **Purpose**: Tells the consumer where to find the Kafka cluster
   - **Value**: `kafka-kafka-bootstrap.kafka-user1.svc.cluster.local:9092`

2. **group.id**
   - **Purpose**: Identifies the consumer group
   - **Value**: `tutorial-consumer-group`
   - **Why it matters**: Consumers with the same group ID share the work of consuming messages

3. **key.deserializer** / **value.deserializer**
   - **Purpose**: Convert bytes received over the network back to Java objects
   - **Value**: `org.apache.kafka.common.serialization.StringDeserializer`

#### Important Optional Parameters

4. **auto.offset.reset** (Demo Parameter)
   - **Purpose**: What to do when there is no initial offset or offset is out of range
   - **Options**:
     - `"earliest"`: Start from the beginning of the topic (read ALL messages)
     - `"latest"`: Start from the end (only read NEW messages)
   - **Default**: `"latest"`
   - **Demo**: 
     - First, run the producer to send some messages
     - Then run the consumer with `"latest"` - you won't see the old messages
     - Run the consumer again with `"earliest"` - you'll see ALL messages from the beginning

5. **enable.auto.commit**
   - **Purpose**: Automatically commit offsets periodically
   - **Options**:
     - `true`: Kafka automatically commits offsets (simpler, less control)
     - `false`: You must manually commit offsets (more control, more complex)
   - **Default**: `true`

#### Additional Parameters (Commented in Code)

- **auto.commit.interval.ms**: How often to auto-commit offsets
- **max.poll.records**: Maximum number of records returned in a single poll
- **max.poll.interval.ms**: Maximum time between polls before considering consumer dead
- **session.timeout.ms**: Timeout for detecting consumer failures
- **fetch.min.bytes**: Minimum bytes to fetch in a request
- **fetch.max.wait.ms**: Maximum time to wait for fetch.min.bytes

## Demo Scenarios

### Demo 1: Producer Batching Behavior

**Objective**: Show how `batch.size` and `linger.ms` affect message batching.

**Steps**:
1. Run the producer with default settings (batch.size=16384, linger.ms=0)
2. Observe the timing and behavior
3. Run the producer again with batch.size=1024, linger.ms=0
4. Compare: Smaller batch size may result in more frequent sends
5. Run the producer with batch.size=16384, linger.ms=100
6. Compare: With linger.ms > 0, messages may be batched together even if batch isn't full

**Expected Observation**: 
- With larger batch.size and/or linger.ms > 0, messages are grouped together for better throughput
- With smaller batch.size and linger.ms=0, messages are sent more immediately

### Demo 2: Consumer Offset Reset Behavior

**Objective**: Show how `auto.offset.reset` affects which messages are consumed.

**Steps**:
1. Run the producer to send 10 messages
2. Run the consumer with `auto.offset.reset=latest`
   - **Result**: Consumer won't see the messages already sent (only new ones)
3. Run the producer again to send 10 more messages
4. Run the consumer with `auto.offset.reset=latest` again
   - **Result**: Consumer will see the new messages
5. Run the consumer with `auto.offset.reset=earliest`
   - **Result**: Consumer will see ALL messages from the beginning of the topic

**Expected Observation**:
- `latest`: Only consumes messages sent after the consumer starts
- `earliest`: Consumes all messages from the beginning of the topic

## Troubleshooting

### Connection Issues

If you see connection errors:
- Verify the Kafka cluster is accessible from your environment
- Check the bootstrap server address: `kafka-kafka-bootstrap.kafka-user1.svc.cluster.local:9092`
- Ensure network connectivity to the Kafka namespace

### Topic Not Found

If you see "Topic does not exist" errors:
- The topic `tutorial-topic` will be auto-created if auto-creation is enabled
- Alternatively, create the topic manually:
  ```bash
  kafka-topics.sh --create --topic tutorial-topic --bootstrap-server kafka-kafka-bootstrap.kafka-user1.svc.cluster.local:9092
  ```

### No Messages Received

- If using `auto.offset.reset=latest`, make sure to send messages AFTER starting the consumer
- If using `auto.offset.reset=earliest`, ensure messages exist in the topic

## Learning Objectives

After completing this tutorial, you should understand:

1. How to create a Kafka Producer in Java
2. How to create a Kafka Consumer in Java
3. Key configuration parameters and their effects
4. How batching affects producer performance
5. How offset reset policy affects consumer behavior
6. The difference between `earliest` and `latest` offset reset

## Additional Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Producer Configuration](https://kafka.apache.org/documentation/#producerconfigs)
- [Kafka Consumer Configuration](https://kafka.apache.org/documentation/#consumerconfigs)

## License

This is a tutorial application for educational purposes.

