package com.kafka.tutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
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
        
        ProducerDisplayHelper.printConfiguration(topic, batchSize, lingerMs);
    }
    
    /**
     * Sends sample messages to the topic
     * Demonstrates linger.ms behavior by tracking when messages are queued vs when they're actually sent
     */
    public void sendSampleMessages() {
        System.out.println("Sending " + KafkaConfig.SAMPLE_MESSAGE_COUNT + " sample messages...\n");
        
        long queueStartTime = System.currentTimeMillis();
        List<MessageTiming> messageTimings = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(KafkaConfig.SAMPLE_MESSAGE_COUNT);
        
        // Queue messages
        ProducerDisplayHelper.printQueuingStart();
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
                    timing.partition = metadata.partition();
                    ProducerDisplayHelper.printMessageSent(timing, metadata, queueStartTime);
                } else {
                    ProducerDisplayHelper.printMessageError(timing.messageNumber, exception.getMessage());
                }
                latch.countDown();
            });
        }
        
        long queueDuration = System.currentTimeMillis() - queueStartTime;
        ProducerDisplayHelper.printQueueComplete(KafkaConfig.SAMPLE_MESSAGE_COUNT, queueDuration);
        
        ProducerDisplayHelper.printLingerExplanation(lingerMs);
        
        // Wait for all callbacks to complete
        // This respects linger.ms - callbacks fire when Kafka actually sends messages
        // DO NOT call flush() here - it would force immediate sending and bypass linger.ms!
        try {
            latch.await(); // Blocks until all callbacks complete (respects linger.ms naturally)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Analyze batching behavior
        BatchingAnalyzer.analyzeAndPrint(messageTimings);
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

