package com.kafka.tutorial;

import java.util.Scanner;

/**
 * Main application class with interactive menu
 * 
 * This application demonstrates Kafka Producer and Consumer basics
 * with configurable parameters to show behavior differences.
 */
public class KafkaTutorialApp {
    
    private static Scanner scanner = new Scanner(System.in);
    
    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("   Kafka Java Tutorial Application");
        System.out.println("========================================\n");
        
        boolean running = true;
        
        while (running) {
            showMainMenu();
            int choice = getIntInput("Enter your choice: ");
            
            switch (choice) {
                case 1:
                    runProducer();
                    break;
                case 2:
                    runConsumer();
                    break;
                case 3:
                    System.out.println("Exiting application. Goodbye!");
                    running = false;
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.\n");
            }
        }
        
        scanner.close();
    }
    
    /**
     * Displays the main menu
     */
    private static void showMainMenu() {
        System.out.println("Main Menu:");
        System.out.println("1. Run Producer");
        System.out.println("2. Run Consumer");
        System.out.println("3. Exit");
        System.out.println();
    }
    
    /**
     * Runs the producer with configuration options
     */
    private static void runProducer() {
        System.out.println("\n=== Producer Configuration ===");
        
        int batchSize = getBatchSize();
        int lingerMs = getLingerMs();
        
        System.out.println("\nStarting Producer...\n");
        
        SimpleKafkaProducer producer = null;
        try {
            producer = new SimpleKafkaProducer(KafkaConfig.DEFAULT_TOPIC, batchSize, lingerMs);
            producer.sendSampleMessages();
        } catch (Exception e) {
            System.err.println("Error running producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (producer != null) {
                producer.close();
            }
        }
        
        waitForEnter();
    }
    
    private static int getBatchSize() {
        System.out.println("\nBatch Size Configuration:");
        System.out.println("  Batch size determines how many bytes of messages are grouped together");
        System.out.println("  before sending to Kafka.");
        System.out.println("  - Larger values (e.g., 32768) = better throughput, more batching");
        System.out.println("  - Smaller values (e.g., 1024) = lower latency, less batching");
        System.out.println("  Default: " + KafkaConfig.DEFAULT_BATCH_SIZE + " bytes");
        
        int batchSize = getIntInput("Enter batch size in bytes (or press Enter for default): ");
        return batchSize <= 0 ? KafkaConfig.DEFAULT_BATCH_SIZE : batchSize;
    }
    
    private static int getLingerMs() {
        System.out.println("\nLinger.ms Configuration:");
        System.out.println("  Linger.ms is the time to wait before sending a batch.");
        System.out.println("  - 0 = send immediately (lower latency)");
        System.out.println("  - >0 (e.g., 100) = wait to accumulate more messages (better throughput)");
        System.out.println("  Default: " + KafkaConfig.DEFAULT_LINGER_MS + " ms");
        
        int lingerMs = getIntInput("Enter linger.ms in milliseconds (or press Enter for default): ");
        return lingerMs < 0 ? KafkaConfig.DEFAULT_LINGER_MS : lingerMs;
    }
    
    /**
     * Runs the consumer with configuration options
     */
    private static void runConsumer() {
        System.out.println("\n=== Consumer Configuration ===");
        
        String autoOffsetReset = getAutoOffsetReset();
        int fetchMinBytes = getFetchMinBytes();
        int fetchMaxWaitMs = getFetchMaxWaitMs();
        int maxMessages = getMaxMessages();
        
        System.out.println("\nStarting Consumer...\n");
        
        SimpleKafkaConsumer consumer = null;
        try {
            consumer = new SimpleKafkaConsumer(KafkaConfig.DEFAULT_TOPIC, autoOffsetReset, fetchMinBytes, fetchMaxWaitMs);
            consumer.consumeMessages(maxMessages);
        } catch (Exception e) {
            System.err.println("Error running consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        
        waitForEnter();
    }
    
    private static String getAutoOffsetReset() {
        System.out.println("\nAuto Offset Reset Configuration:");
        System.out.println("  This determines where the consumer starts reading from the topic.");
        System.out.println("  - \"earliest\" = read from the beginning (see ALL existing messages)");
        System.out.println("  - \"latest\" = only read NEW messages sent after consumer starts");
        System.out.println("  Default: " + KafkaConfig.DEFAULT_AUTO_OFFSET_RESET);
        System.out.println("\n  DEMO: Try 'earliest' to see all messages, or 'latest' to only see new ones!");
        
        String input = getStringInput("Enter auto.offset.reset (earliest/latest, or press Enter for default): ");
        if (input == null || input.isEmpty()) {
            return KafkaConfig.DEFAULT_AUTO_OFFSET_RESET;
        }
        
        String normalized = input.toLowerCase().trim();
        if ("earliest".equals(normalized) || "latest".equals(normalized)) {
            return normalized;
        }
        
        System.out.println("Invalid value. Must be 'earliest' or 'latest'. Using default: " + KafkaConfig.DEFAULT_AUTO_OFFSET_RESET);
        return KafkaConfig.DEFAULT_AUTO_OFFSET_RESET;
    }
    
    private static int getFetchMinBytes() {
        System.out.println("\nFetch Min Bytes Configuration:");
        System.out.println("  This determines the minimum bytes to accumulate before returning from fetch.");
        System.out.println("  - Smaller values (e.g., 1) = return immediately, lower latency");
        System.out.println("  - Larger values (e.g., 1024) = wait to accumulate more data, better batching");
        System.out.println("  Default: " + KafkaConfig.DEFAULT_FETCH_MIN_BYTES);
        System.out.println("\n  DEMO: Try 1 for immediate return, or 1024 to see batching behavior!");
        
        int fetchMinBytes = getIntInput("Enter fetch.min.bytes (or press Enter for default): ");
        return fetchMinBytes <= 0 ? KafkaConfig.DEFAULT_FETCH_MIN_BYTES : fetchMinBytes;
    }
    
    private static int getFetchMaxWaitMs() {
        System.out.println("\nFetch Max Wait Ms Configuration:");
        System.out.println("  This determines the maximum time to wait for fetch.min.bytes.");
        System.out.println("  - Smaller values (e.g., 100) = less waiting, lower latency");
        System.out.println("  - Larger values (e.g., 2000) = more time to accumulate data, better batching");
        System.out.println("  Default: " + KafkaConfig.DEFAULT_FETCH_MAX_WAIT_MS + " ms");
        System.out.println("\n  NOTE: Returns when min bytes reached OR max wait time elapsed (whichever comes first)");
        System.out.println("  DEMO: Try 100 for quick return, or 2000 to see waiting behavior!");
        
        int fetchMaxWaitMs = getIntInput("Enter fetch.max.wait.ms (or press Enter for default): ");
        return fetchMaxWaitMs <= 0 ? KafkaConfig.DEFAULT_FETCH_MAX_WAIT_MS : fetchMaxWaitMs;
    }
    
    private static int getMaxMessages() {
        System.out.println("\nHow many messages to consume?");
        System.out.println("  Enter 0 for unlimited (will keep consuming until you stop it)");
        int maxMessages = getIntInput("Enter number of messages (or press Enter for 0): ");
        return maxMessages < 0 ? 0 : maxMessages;
    }
    
    private static void waitForEnter() {
        System.out.println("\nPress Enter to return to main menu...");
        scanner.nextLine();
    }
    
    /**
     * Gets integer input from user
     * Returns -1 if input is empty (allowing default values)
     */
    private static int getIntInput(String prompt) {
        System.out.print(prompt);
        String input = scanner.nextLine().trim();
        if (input.isEmpty()) {
            return -1; // Signal to use default
        }
        try {
            return Integer.parseInt(input);
        } catch (NumberFormatException e) {
            System.out.println("Invalid number. Using default value.");
            return -1;
        }
    }
    
    /**
     * Gets string input from user
     * Returns null if input is empty (allowing default values)
     */
    private static String getStringInput(String prompt) {
        System.out.print(prompt);
        String input = scanner.nextLine().trim();
        return input.isEmpty() ? null : input;
    }
}

