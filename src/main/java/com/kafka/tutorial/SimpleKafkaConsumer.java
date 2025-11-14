package com.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

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
        
        ConsumerDisplayHelper.printConfiguration(topic, autoOffsetReset, fetchMinBytes, fetchMaxWaitMs);
        ConsumerDisplayHelper.printConfigurationExplanation(autoOffsetReset, fetchMinBytes, fetchMaxWaitMs);
    }
    
    /**
     * Consumes messages from the topic
     * 
     * @param maxMessages Maximum number of messages to consume (0 = unlimited)
     */
    public void consumeMessages(int maxMessages) {
        ConsumerDisplayHelper.printConsumptionStartMessage(maxMessages);
        
        int messageCount = 0;
        int emptyPollCount = 0;
        final int MAX_EMPTY_POLLS = 10;
        
        try {
            while (maxMessages == 0 || messageCount < maxMessages) {
                // Poll for new messages
                // This is a blocking call that waits up to the specified duration
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    emptyPollCount++;
                    if (maxMessages > 0 && emptyPollCount >= MAX_EMPTY_POLLS) {
                        ConsumerDisplayHelper.printNoMessagesAfterMaxPolls(MAX_EMPTY_POLLS);
                        break;
                    }
                    ConsumerDisplayHelper.printEmptyPollMessage(maxMessages, emptyPollCount, MAX_EMPTY_POLLS);
                    continue;
                }
                
                emptyPollCount = 0;
                ConsumerDisplayHelper.printPollResults(records.count(), fetchMinBytes, fetchMaxWaitMs);
                
                // Process each record
                for (ConsumerRecord<String, String> record : records) {
                    messageCount++;
                    ConsumerDisplayHelper.printMessage(messageCount, record);
                    
                    if (maxMessages > 0 && messageCount >= maxMessages) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error while consuming messages: " + e.getMessage());
            e.printStackTrace();
        } finally {
            ConsumerDisplayHelper.printSummary(messageCount, autoOffsetReset, fetchMinBytes, fetchMaxWaitMs);
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

