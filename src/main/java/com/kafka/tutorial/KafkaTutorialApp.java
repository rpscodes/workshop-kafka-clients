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
        
        // Get batch size configuration
        System.out.println("\nBatch Size Configuration:");
        System.out.println("  Batch size determines how many bytes of messages are grouped together");
        System.out.println("  before sending to Kafka.");
        System.out.println("  - Larger values (e.g., 32768) = better throughput, more batching");
        System.out.println("  - Smaller values (e.g., 1024) = lower latency, less batching");
        System.out.println("  Default: " + KafkaConfig.DEFAULT_BATCH_SIZE + " bytes");
        
        int batchSize = getIntInput("Enter batch size in bytes (or press Enter for default): ");
        if (batchSize <= 0) {
            batchSize = KafkaConfig.DEFAULT_BATCH_SIZE;
        }
        
        // Get linger.ms configuration
        System.out.println("\nLinger.ms Configuration:");
        System.out.println("  Linger.ms is the time to wait before sending a batch.");
        System.out.println("  - 0 = send immediately (lower latency)");
        System.out.println("  - >0 (e.g., 100) = wait to accumulate more messages (better throughput)");
        System.out.println("  Default: " + KafkaConfig.DEFAULT_LINGER_MS + " ms");
        
        int lingerMs = getIntInput("Enter linger.ms in milliseconds (or press Enter for default): ");
        if (lingerMs < 0) {
            lingerMs = KafkaConfig.DEFAULT_LINGER_MS;
        }
        
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
        
        System.out.println("\nPress Enter to return to main menu...");
        scanner.nextLine();
    }
    
    /**
     * Runs the consumer with configuration options
     */
    private static void runConsumer() {
        System.out.println("\n=== Consumer Configuration ===");
        
        // Get max.poll.records configuration
        System.out.println("\nMax Poll Records Configuration:");
        System.out.println("  This determines the maximum number of records returned in a single poll.");
        System.out.println("  - Smaller values (e.g., 1) = fewer messages per poll, more poll calls needed");
        System.out.println("  - Larger values (e.g., 10) = more messages per poll, fewer poll calls needed");
        System.out.println("  Default: " + KafkaConfig.DEFAULT_MAX_POLL_RECORDS);
        System.out.println("\n  DEMO: Try 1 to see one message per poll, or 10 to see multiple messages per poll!");
        
        int maxPollRecords = getIntInput("Enter max.poll.records (or press Enter for default): ");
        if (maxPollRecords <= 0) {
            maxPollRecords = KafkaConfig.DEFAULT_MAX_POLL_RECORDS;
        }
        
        // Get max messages to consume
        System.out.println("\nHow many messages to consume?");
        System.out.println("  Enter 0 for unlimited (will keep consuming until you stop it)");
        int maxMessages = getIntInput("Enter number of messages (or press Enter for 0): ");
        if (maxMessages < 0) {
            maxMessages = 0;
        }
        
        System.out.println("\nStarting Consumer...\n");
        
        SimpleKafkaConsumer consumer = null;
        try {
            consumer = new SimpleKafkaConsumer(KafkaConfig.DEFAULT_TOPIC, maxPollRecords);
            consumer.consumeMessages(maxMessages);
        } catch (Exception e) {
            System.err.println("Error running consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        
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

