package com.kafka.tutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Simple Kafka Producer class demonstrating key configuration parameters
 * 
 * This class shows:
 * - How to configure a Kafka producer
 * - Important configuration parameters and their effects
 * - How batch.size and linger.ms affect batching behavior
 */
public class SimpleKafkaProducer {
    
    private KafkaProducer<String, String> producer;
    private String topic;
    private int batchSize;
    private int lingerMs;
    
    /**
     * Creates a new Kafka Producer with the specified configuration
     * 
     * @param topic The topic to send messages to
     * @param batchSize Batch size in bytes (affects batching behavior)
     * @param lingerMs Time to wait before sending a batch in milliseconds
     */
    public SimpleKafkaProducer(String topic, int batchSize, int lingerMs) {
        this.topic = topic;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        
        // Create producer properties
        Properties props = new Properties();
        
        // REQUIRED: Bootstrap servers - tells producer where to find Kafka cluster
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        
        // REQUIRED: Serializers for key and value
        // These convert Java objects to bytes for transmission over the network
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConfig.KEY_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaConfig.VALUE_SERIALIZER);
        
        // ACKS: Controls producer reliability
        // "all" = wait for all in-sync replicas to acknowledge (most reliable, slower)
        // "1" = wait for leader to acknowledge (balanced)
        // "0" = don't wait for acknowledgment (fastest, least reliable)
        props.put(ProducerConfig.ACKS_CONFIG, KafkaConfig.ACKS);
        
        // BATCH.SIZE: Maximum size of a batch in bytes
        // Larger batches = better throughput, but more memory usage
        // Smaller batches = lower latency, but less efficient
        // DEMO: Try changing this to see batching behavior
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        
        // LINGER.MS: Time to wait before sending a batch
        // 0 = send immediately (lower latency)
        // >0 = wait to accumulate more messages (better throughput)
        // DEMO: Try changing this to see batching behavior
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        
        // Additional useful configurations (commented for reference)
        // props.put(ProducerConfig.RETRIES_CONFIG, 3); // Number of retries on failure
        // props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // Parallelism
        // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Compression
        // props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // Total memory for buffering
        
        // Create the producer instance
        this.producer = new KafkaProducer<>(props);
        
        System.out.println("\n=== Producer Configuration ===");
        System.out.println("Bootstrap Servers: " + KafkaConfig.BOOTSTRAP_SERVERS);
        System.out.println("Topic: " + topic);
        System.out.println("Batch Size: " + batchSize + " bytes");
        System.out.println("Linger MS: " + lingerMs + " ms");
        System.out.println("Acks: " + KafkaConfig.ACKS);
        System.out.println("==============================\n");
    }
    
    /**
     * Sends sample messages to the topic
     * Demonstrates linger.ms behavior by tracking when messages are queued vs when they're actually sent
     */
    public void sendSampleMessages() {
        System.out.println("Sending " + KafkaConfig.SAMPLE_MESSAGE_COUNT + " sample messages...\n");
        
        long startTime = System.currentTimeMillis();
        long queueStartTime = startTime;
        
        // Track timing for each message to demonstrate batching behavior
        List<MessageTiming> messageTimings = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(KafkaConfig.SAMPLE_MESSAGE_COUNT);
        
        System.out.println("=== Queuing Messages ===");
        for (int i = 1; i <= KafkaConfig.SAMPLE_MESSAGE_COUNT; i++) {
            String key = "key-" + i;
            String value = "Message " + i;
            
            long queueTime = System.currentTimeMillis();
            MessageTiming timing = new MessageTiming(i, queueTime);
            messageTimings.add(timing);
            
            // Create a producer record (message)
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            
            // Send the message (asynchronous by default)
            // Kafka will batch messages according to linger.ms and batch.size
            producer.send(record, (metadata, exception) -> {
                long sendTime = System.currentTimeMillis();
                timing.sendTime = sendTime;
                timing.delay = sendTime - timing.queueTime;
                
                if (exception == null) {
                    timing.partition = metadata.partition(); // Store partition for batching analysis
                    System.out.println(String.format(
                        "✓ Message %d sent: Queued at %d ms, Sent at %d ms, Delay: %d ms (partition: %d, offset: %d)",
                        timing.messageNumber, timing.queueTime - queueStartTime, 
                        timing.sendTime - queueStartTime, timing.delay, metadata.partition(), metadata.offset()
                    ));
                } else {
                    System.err.println("Error sending message " + timing.messageNumber + ": " + exception.getMessage());
                }
                latch.countDown();
            });
        }
        
        long queueEndTime = System.currentTimeMillis();
        long queueDuration = queueEndTime - queueStartTime;
        
        System.out.println("\nAll " + KafkaConfig.SAMPLE_MESSAGE_COUNT + " messages queued in " + queueDuration + " ms");
        
        if (lingerMs > 0) {
            System.out.println("\n=== Waiting for linger.ms to expire ===");
            System.out.println("With linger.ms=" + lingerMs + " ms, Kafka will automatically:");
            System.out.println("  - Wait up to " + lingerMs + " ms to accumulate messages into batches");
            System.out.println("  - Send batches when linger.ms expires OR batch.size is reached");
            System.out.println("  - This improves throughput by batching multiple messages together");
            System.out.println("\nWaiting for Kafka to send messages (callbacks will fire when linger.ms expires)...\n");
        } else {
            System.out.println("\nWith linger.ms=0, messages will be sent immediately (no batching delay)");
            System.out.println("Waiting for callbacks to complete...\n");
        }
        
        // Wait for all callbacks to complete
        // This respects linger.ms - callbacks fire when Kafka actually sends messages
        // DO NOT call flush() here - it would force immediate sending and bypass linger.ms!
        long waitStartTime = System.currentTimeMillis();
        try {
            latch.await(); // Blocks until all callbacks complete (respects linger.ms naturally)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        long waitEndTime = System.currentTimeMillis();
        
        // All messages have been sent and acknowledged (all callbacks fired)
        // No need to call flush() - Kafka has already sent everything according to linger.ms
        
        long endTime = System.currentTimeMillis();
        long totalDuration = endTime - startTime;
        long waitDuration = waitEndTime - waitStartTime;
        
        // Analyze batching behavior
        analyzeBatching(messageTimings, queueStartTime);
    }
    
    /**
     * Analyzes and displays batching behavior based on message send times
     * Groups messages by partition AND send time (batches are per partition)
     */
    private void analyzeBatching(List<MessageTiming> timings, long startTime) {
        System.out.println("\n=== Batching Analysis ===");
        System.out.println("Note: Batches are per partition - messages in different partitions cannot be in the same batch\n");
        
        // Group messages by partition first, then by send time within each partition
        // Batches are per partition - messages in different partitions cannot be in the same batch
        Map<Integer, List<MessageTiming>> byPartition = new HashMap<>();
        
        for (MessageTiming timing : timings) {
            if (timing.partition >= 0) { // Only process successfully sent messages
                byPartition.computeIfAbsent(timing.partition, k -> new ArrayList<>()).add(timing);
            }
        }
        
        int totalBatches = 0;
        long batchWindow = 50; // ms - messages sent within this window on same partition are likely batched
        
        for (Map.Entry<Integer, List<MessageTiming>> partitionEntry : byPartition.entrySet()) {
            int partition = partitionEntry.getKey();
            List<MessageTiming> partitionMessages = partitionEntry.getValue();
            
            // Sort by send time
            partitionMessages.sort((a, b) -> Long.compare(a.sendTime, b.sendTime));
            
            // Group by send time within this partition
            List<List<MessageTiming>> partitionBatches = new ArrayList<>();
            
            for (MessageTiming timing : partitionMessages) {
                boolean addedToBatch = false;
                for (List<MessageTiming> batch : partitionBatches) {
                    MessageTiming lastInBatch = batch.get(batch.size() - 1);
                    // Same partition AND sent within batchWindow
                    if (Math.abs(timing.sendTime - lastInBatch.sendTime) <= batchWindow) {
                        batch.add(timing);
                        addedToBatch = true;
                        break;
                    }
                }
                if (!addedToBatch) {
                    List<MessageTiming> newBatch = new ArrayList<>();
                    newBatch.add(timing);
                    partitionBatches.add(newBatch);
                }
            }
            
            totalBatches += partitionBatches.size();
            
            System.out.println("Partition " + partition + ":");
            for (int i = 0; i < partitionBatches.size(); i++) {
                List<MessageTiming> batch = partitionBatches.get(i);
                System.out.println("  Batch " + (i + 1) + ": " + batch.size() + " message(s)");
            }
            System.out.println();
        }
        
        System.out.println("Total batches across all partitions: " + totalBatches);
        System.out.println("========================");
    }
    
    /**
     * Helper class to track message timing
     */
    private static class MessageTiming {
        int messageNumber;
        long queueTime;
        long sendTime;
        long delay;
        int partition = -1; // -1 means not yet assigned
        
        MessageTiming(int messageNumber, long queueTime) {
            this.messageNumber = messageNumber;
            this.queueTime = queueTime;
        }
    }
    
    /**
     * Closes the producer and releases resources
     */
    public void close() {
        if (producer != null) {
            producer.close();
            System.out.println("Producer closed successfully.");
        }
    }
}

