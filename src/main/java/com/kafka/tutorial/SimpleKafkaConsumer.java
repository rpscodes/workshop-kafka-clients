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
 * - How max.poll.records affects batching behavior (how many messages per poll)
 */
public class SimpleKafkaConsumer {
    
    private KafkaConsumer<String, String> consumer;
    private String topic;
    private int maxPollRecords;
    
    /**
     * Creates a new Kafka Consumer with the specified configuration
     * 
     * @param topic The topic to consume messages from
     * @param maxPollRecords Maximum number of records returned in a single poll
     */
    public SimpleKafkaConsumer(String topic, int maxPollRecords) {
        this.topic = topic;
        this.maxPollRecords = maxPollRecords;
        
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
        // "earliest" = start from the beginning of the topic (read all messages)
        // "latest" = start from the end (only read new messages)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConfig.AUTO_OFFSET_RESET);
        
        // ENABLE.AUTO.COMMIT: Automatically commit offsets periodically
        // true = Kafka automatically commits offsets (simpler, but less control)
        // false = You must manually commit offsets (more control, more complex)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, KafkaConfig.ENABLE_AUTO_COMMIT);
        
        // MAX.POLL.RECORDS: Maximum number of records returned in a single poll
        // Smaller values (e.g., 1) = fewer messages per poll, more poll calls needed
        // Larger values (e.g., 10) = more messages per poll, fewer poll calls needed
        // DEMO: Try changing this to see batching behavior!
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        
        // Additional useful configurations (commented for reference)
        // props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000); // Auto-commit interval
        // props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // Max time between polls
        // props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000); // Session timeout
        // props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1); // Minimum bytes to fetch
        // props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500); // Max wait time for fetch
        
        // Create the consumer instance
        this.consumer = new KafkaConsumer<>(props);
        
        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(topic));
        
        System.out.println("\n=== Consumer Configuration ===");
        System.out.println("Bootstrap Servers: " + KafkaConfig.BOOTSTRAP_SERVERS);
        System.out.println("Topic: " + topic);
        System.out.println("Group ID: " + KafkaConfig.GROUP_ID);
        System.out.println("Auto Offset Reset: " + KafkaConfig.AUTO_OFFSET_RESET);
        System.out.println("Max Poll Records: " + maxPollRecords);
        System.out.println("Enable Auto Commit: " + KafkaConfig.ENABLE_AUTO_COMMIT);
        System.out.println("================================\n");
        
        System.out.println("NOTE: With max.poll.records=" + maxPollRecords + ", the consumer will");
        if (maxPollRecords == 1) {
            System.out.println("      receive 1 message per poll call (many polls needed).\n");
        } else {
            System.out.println("      receive up to " + maxPollRecords + " messages per poll call (fewer polls needed).\n");
        }
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
                
                // Show how many messages were returned in this poll (demonstrates max.poll.records)
                int recordsInPoll = records.count();
                System.out.println(">>> Poll returned " + recordsInPoll + " message(s) (max.poll.records=" + maxPollRecords + ") <<<\n");
                
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
            System.out.println("Max Poll Records used: " + maxPollRecords);
            if (maxPollRecords == 1) {
                System.out.println("Messages were received one at a time (1 per poll).");
            } else {
                System.out.println("Messages were batched (up to " + maxPollRecords + " per poll).");
            }
            if (messageCount == 0) {
                System.out.println("\nNo messages were consumed. This could mean:");
                System.out.println("  - The topic is empty");
                System.out.println("  - No new messages were sent after consumer started");
                System.out.println("  - Try running the producer first to send some messages");
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

