package com.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Simple Kafka Consumer class demonstrating key configuration parameters
 * 
 * This class shows:
 * - How to configure a Kafka consumer
 * - Important configuration parameters and their effects
 * - How auto.offset.reset controls where the consumer starts reading from
 * - How fetch.min.bytes and fetch.max.wait.ms control fetch batching behavior
 */
public class SimpleKafkaConsumer {
    
    private KafkaConsumer<String, String> consumer;
    private String topic;
    private String autoOffsetReset;
    private int fetchMinBytes;
    private int fetchMaxWaitMs;
    
    /**
     * Creates a new Kafka Consumer with the specified configuration
     * 
     * @param topic The topic to consume messages from
     * @param autoOffsetReset Where to start reading from: "earliest" or "latest"
     * @param fetchMinBytes Minimum bytes to accumulate before returning from fetch
     * @param fetchMaxWaitMs Maximum time to wait for fetch.min.bytes before returning
     */
    public SimpleKafkaConsumer(String topic, String autoOffsetReset, int fetchMinBytes, int fetchMaxWaitMs) {
        this.topic = topic;
        this.autoOffsetReset = autoOffsetReset;
        this.fetchMinBytes = fetchMinBytes;
        this.fetchMaxWaitMs = fetchMaxWaitMs;
        
        // Create consumer properties
        Properties props = new Properties();
        
        // REQUIRED: Bootstrap servers - tells consumer where to find Kafka cluster
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        
        // REQUIRED: Consumer group ID
        // Consumers with the same group ID share the work of consuming messages
        // Each message is delivered to only one consumer in the group
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.GROUP_ID);
        
        // REQUIRED: Deserializers for key and value
        // These convert bytes received over the network back to Java objects
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConfig.KEY_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConfig.VALUE_DESERIALIZER);
        
        // AUTO.OFFSET.RESET: What to do when there is no initial offset or offset is out of range
        // "earliest" = start from the beginning of the topic (read all messages from the start)
        // "latest" = start from the end (only read new messages sent after consumer starts)
        // DEMO: Try "earliest" to see all messages, or "latest" to only see new ones!
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        
        // ENABLE.AUTO.COMMIT: Automatically commit offsets periodically
        // true = Kafka automatically commits offsets (simpler, but less control)
        // false = You must manually commit offsets (more control, more complex)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaConfig.ENABLE_AUTO_COMMIT);
        
        // FETCH.MIN.BYTES: Minimum amount of data the server should return
        // The consumer will wait until at least this many bytes are available
        // Larger values = more batching at the fetch stage, better throughput
        // Smaller values = less waiting, lower latency
        // Works with fetch.max.wait.ms - waits for min bytes OR max wait time, whichever comes first
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        
        // FETCH.MAX.WAIT.MS: Maximum time to wait for fetch.min.bytes
        // If fetch.min.bytes isn't reached within this time, return whatever is available
        // Works together with fetch.min.bytes to control batching vs latency
        // Larger values = more time to accumulate data, better batching
        // Smaller values = less waiting, lower latency
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        
        // Additional useful configurations (commented for reference)
        // props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000); // Auto-commit interval
        // props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // Max time between polls
        // props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000); // Session timeout
        
        // Create the consumer instance
        this.consumer = new KafkaConsumer<>(props);
        
        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(topic));
        
        System.out.println("\n=== Consumer Configuration ===");
        System.out.println("Bootstrap Servers: " + KafkaConfig.BOOTSTRAP_SERVERS);
        System.out.println("Topic: " + topic);
        System.out.println("Group ID: " + KafkaConfig.GROUP_ID);
        System.out.println("Auto Offset Reset: " + autoOffsetReset);
        System.out.println("Fetch Min Bytes: " + fetchMinBytes);
        System.out.println("Fetch Max Wait Ms: " + fetchMaxWaitMs);
        System.out.println("Enable Auto Commit: " + KafkaConfig.ENABLE_AUTO_COMMIT);
        System.out.println("================================\n");
        
        System.out.println("CONFIGURATION EXPLANATION:");
        System.out.println("  Auto Offset Reset (" + autoOffsetReset + "):");
        if ("earliest".equals(autoOffsetReset)) {
            System.out.println("    - Consumer will read from the beginning of the topic");
            System.out.println("    - You'll see ALL messages that exist in the topic");
        } else {
            System.out.println("    - Consumer will only read NEW messages sent after it starts");
            System.out.println("    - You won't see messages that were sent before the consumer started");
        }
        System.out.println("  Fetch Batching (fetch.min.bytes + fetch.max.wait.ms):");
        System.out.println("    - Waits up to " + fetchMaxWaitMs + "ms to accumulate at least " + fetchMinBytes + " bytes");
        System.out.println("    - Fetches data from broker into consumer's internal buffer");
        System.out.println("    - Returns when min bytes reached OR max wait time elapsed (whichever comes first)");
        System.out.println("    - Larger values = more batching, better throughput");
        System.out.println("    - Smaller values = less waiting, lower latency");
        System.out.println();
    }
    
    /**
     * Consumes messages from the topic
     * 
     * @param maxMessages Maximum number of messages to consume (0 = unlimited)
     */
    public void consumeMessages(int maxMessages) {
        System.out.println("Starting to consume messages...");
        if (maxMessages > 0) {
            System.out.println("Will consume up to " + maxMessages + " messages.");
        } else {
            System.out.println("Will consume messages until stopped (Press Ctrl+C to stop).\n");
        }
        
        int messageCount = 0;
        boolean shouldContinue = true;
        int emptyPollCount = 0;
        final int MAX_EMPTY_POLLS = 10; // Stop after 10 empty polls if maxMessages is set
        
        try {
            while (shouldContinue) {
                // Poll for new messages
                // This is a blocking call that waits up to the specified duration
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    emptyPollCount++;
                    
                    // If we have a max message limit and no messages after several polls, exit
                    if (maxMessages > 0 && emptyPollCount >= MAX_EMPTY_POLLS) {
                        System.out.println("\nNo messages found after " + MAX_EMPTY_POLLS + " attempts.");
                        System.out.println("The topic appears to be empty or no messages are available.");
                        System.out.println("Try running the producer first to send some messages.");
                        shouldContinue = false;
                        break;
                    }
                    
                    // For unlimited mode, only print every 5 polls to avoid spam
                    if (maxMessages == 0 && emptyPollCount % 5 == 0) {
                        System.out.println("No messages received yet. Waiting... (poll #" + emptyPollCount + ")");
                    } else if (maxMessages > 0) {
                        System.out.println("No messages received in this poll. Waiting... (attempt " + emptyPollCount + "/" + MAX_EMPTY_POLLS + ")");
                    }
                    continue;
                }
                
                // Reset empty poll count when we receive messages
                emptyPollCount = 0;
                
                // Show how many messages were returned in this poll
                int recordsInPoll = records.count();
                System.out.println(">>> Poll returned " + recordsInPoll + " message(s) <<<");
                System.out.println("    (fetch.min.bytes=" + fetchMinBytes + ", fetch.max.wait.ms=" + fetchMaxWaitMs + ")\n");
                
                // Process each record
                for (ConsumerRecord<String, String> record : records) {
                    messageCount++;
                    
                    System.out.println("=== Message " + messageCount + " ===");
                    System.out.println("Topic: " + record.topic());
                    System.out.println("Partition: " + record.partition());
                    System.out.println("Offset: " + record.offset());
                    System.out.println("Key: " + record.key());
                    System.out.println("Value: " + record.value());
                    System.out.println("Timestamp: " + record.timestamp());
                    System.out.println("========================\n");
                    
                    // Stop if we've reached the max message count
                    if (maxMessages > 0 && messageCount >= maxMessages) {
                        shouldContinue = false;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error while consuming messages: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("\n=== Consumer Summary ===");
            System.out.println("Total messages consumed: " + messageCount);
            System.out.println("Configuration used:");
            System.out.println("  - auto.offset.reset: " + autoOffsetReset);
            System.out.println("  - fetch.min.bytes: " + fetchMinBytes);
            System.out.println("  - fetch.max.wait.ms: " + fetchMaxWaitMs);
            if (messageCount == 0) {
                System.out.println("\nNo messages were consumed. This could mean:");
                if ("latest".equals(autoOffsetReset)) {
                    System.out.println("  - auto.offset.reset is set to 'latest' (only reads new messages)");
                    System.out.println("  - No new messages were sent after consumer started");
                    System.out.println("  - Try: Run producer first, then consumer, or use 'earliest' to read existing messages");
                } else {
                    System.out.println("  - The topic is empty");
                    System.out.println("  - Try running the producer first to send some messages");
                }
            }
            System.out.println("=======================\n");
        }
    }
    
    /**
     * Closes the consumer and releases resources
     */
    public void close() {
        if (consumer != null) {
            consumer.close();
            System.out.println("Consumer closed successfully.");
        }
    }
}

