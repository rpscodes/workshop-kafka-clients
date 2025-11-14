package com.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Helper class for displaying consumer-related information
 * Separated from SimpleKafkaConsumer to keep the main class focused on Kafka configuration
 */
public class ConsumerDisplayHelper {
    
    public static void printConfiguration(String topic, String autoOffsetReset, int fetchMinBytes, int fetchMaxWaitMs) {
        System.out.println("\n=== Consumer Configuration ===");
        System.out.println("Bootstrap Servers: " + KafkaConfig.BOOTSTRAP_SERVERS);
        System.out.println("Topic: " + topic);
        System.out.println("Group ID: " + KafkaConfig.GROUP_ID);
        System.out.println("Auto Offset Reset: " + autoOffsetReset);
        System.out.println("Fetch Min Bytes: " + fetchMinBytes);
        System.out.println("Fetch Max Wait Ms: " + fetchMaxWaitMs);
        System.out.println("Enable Auto Commit: " + KafkaConfig.ENABLE_AUTO_COMMIT);
        System.out.println("================================\n");
    }
    
    public static void printConfigurationExplanation(String autoOffsetReset, int fetchMinBytes, int fetchMaxWaitMs) {
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
    
    public static void printConsumptionStartMessage(int maxMessages) {
        System.out.println("Starting to consume messages...");
        if (maxMessages > 0) {
            System.out.println("Will consume up to " + maxMessages + " messages.");
        } else {
            System.out.println("Will consume messages until stopped (Press Ctrl+C to stop).\n");
        }
    }
    
    public static void printEmptyPollMessage(int maxMessages, int emptyPollCount, int maxEmptyPolls) {
        if (maxMessages == 0 && emptyPollCount % 5 == 0) {
            System.out.println("No messages received yet. Waiting... (poll #" + emptyPollCount + ")");
        } else if (maxMessages > 0) {
            System.out.println("No messages received in this poll. Waiting... (attempt " + emptyPollCount + "/" + maxEmptyPolls + ")");
        }
    }
    
    public static void printNoMessagesAfterMaxPolls(int maxEmptyPolls) {
        System.out.println("\nNo messages found after " + maxEmptyPolls + " attempts.");
        System.out.println("The topic appears to be empty or no messages are available.");
        System.out.println("Try running the producer first to send some messages.");
    }
    
    public static void printPollResults(int recordCount, int fetchMinBytes, int fetchMaxWaitMs) {
        System.out.println(">>> Poll returned " + recordCount + " message(s) <<<");
        System.out.println("    (fetch.min.bytes=" + fetchMinBytes + ", fetch.max.wait.ms=" + fetchMaxWaitMs + ")\n");
    }
    
    public static void printMessage(int messageNumber, ConsumerRecord<String, String> record) {
        System.out.println("=== Message " + messageNumber + " ===");
        System.out.println("Topic: " + record.topic());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Timestamp: " + record.timestamp());
        System.out.println("========================\n");
    }
    
    public static void printSummary(int messageCount, String autoOffsetReset, int fetchMinBytes, int fetchMaxWaitMs) {
        System.out.println("\n=== Consumer Summary ===");
        System.out.println("Total messages consumed: " + messageCount);
        System.out.println("Configuration used:");
        System.out.println("  - auto.offset.reset: " + autoOffsetReset);
        System.out.println("  - fetch.min.bytes: " + fetchMinBytes);
        System.out.println("  - fetch.max.wait.ms: " + fetchMaxWaitMs);
        if (messageCount == 0) {
            printNoMessagesExplanation(autoOffsetReset);
        }
        System.out.println("=======================\n");
    }
    
    private static void printNoMessagesExplanation(String autoOffsetReset) {
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
}

