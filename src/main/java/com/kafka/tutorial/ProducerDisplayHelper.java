package com.kafka.tutorial;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Helper class for displaying producer-related information
 * Separated from SimpleKafkaProducer to keep the main class focused on Kafka configuration
 */
public class ProducerDisplayHelper {
    
    public static void printConfiguration(String topic, int batchSize, int lingerMs) {
        System.out.println("\n=== Producer Configuration ===");
        System.out.println("Bootstrap Servers: " + KafkaConfig.BOOTSTRAP_SERVERS);
        System.out.println("Topic: " + topic);
        System.out.println("Batch Size: " + batchSize + " bytes");
        System.out.println("Linger MS: " + lingerMs + " ms");
        System.out.println("Acks: " + KafkaConfig.ACKS);
        System.out.println("==============================\n");
    }
    
    public static void printQueuingStart() {
        System.out.println("=== Queuing Messages ===");
    }
    
    public static void printMessageSent(MessageTiming timing, RecordMetadata metadata, long queueStartTime) {
        System.out.println(String.format(
            "✓ Message %d sent: Queued at %d ms, Sent at %d ms, Delay: %d ms (partition: %d, offset: %d)",
            timing.messageNumber, timing.queueTime - queueStartTime, 
            timing.sendTime - queueStartTime, timing.delay, metadata.partition(), metadata.offset()
        ));
    }
    
    public static void printMessageError(int messageNumber, String errorMessage) {
        System.err.println("Error sending message " + messageNumber + ": " + errorMessage);
    }
    
    public static void printQueueComplete(int messageCount, long queueDuration) {
        System.out.println("\nAll " + messageCount + " messages queued in " + queueDuration + " ms");
    }
    
    public static void printLingerExplanation(int lingerMs) {
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
    }
}

