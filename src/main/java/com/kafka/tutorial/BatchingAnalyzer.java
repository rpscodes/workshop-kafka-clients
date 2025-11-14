package com.kafka.tutorial;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class for analyzing and displaying batching behavior
 * Separated from SimpleKafkaProducer to keep the main class focused on Kafka configuration
 */
public class BatchingAnalyzer {
    
    private static final long BATCH_WINDOW_MS = 50; // Messages sent within this window on same partition are likely batched
    
    public static void analyzeAndPrint(List<MessageTiming> timings) {
        System.out.println("\n=== Batching Analysis ===");
        System.out.println("Note: Batches are per partition - messages in different partitions cannot be in the same batch\n");
        
        Map<Integer, List<MessageTiming>> byPartition = groupByPartition(timings);
        int totalBatches = analyzePartitionBatches(byPartition);
        
        System.out.println("Total batches across all partitions: " + totalBatches);
        System.out.println("========================");
    }
    
    private static Map<Integer, List<MessageTiming>> groupByPartition(List<MessageTiming> timings) {
        Map<Integer, List<MessageTiming>> byPartition = new HashMap<>();
        for (MessageTiming timing : timings) {
            if (timing.partition >= 0) {
                byPartition.computeIfAbsent(timing.partition, k -> new ArrayList<>()).add(timing);
            }
        }
        return byPartition;
    }
    
    private static int analyzePartitionBatches(Map<Integer, List<MessageTiming>> byPartition) {
        int totalBatches = 0;
        
        for (Map.Entry<Integer, List<MessageTiming>> entry : byPartition.entrySet()) {
            int partition = entry.getKey();
            List<MessageTiming> partitionMessages = entry.getValue();
            
            partitionMessages.sort((a, b) -> Long.compare(a.sendTime, b.sendTime));
            List<List<MessageTiming>> batches = groupIntoBatches(partitionMessages);
            
            totalBatches += batches.size();
            printPartitionBatches(partition, batches);
        }
        
        return totalBatches;
    }
    
    private static List<List<MessageTiming>> groupIntoBatches(List<MessageTiming> messages) {
        List<List<MessageTiming>> batches = new ArrayList<>();
        
        for (MessageTiming timing : messages) {
            boolean addedToBatch = false;
            for (List<MessageTiming> batch : batches) {
                MessageTiming lastInBatch = batch.get(batch.size() - 1);
                if (Math.abs(timing.sendTime - lastInBatch.sendTime) <= BATCH_WINDOW_MS) {
                    batch.add(timing);
                    addedToBatch = true;
                    break;
                }
            }
            if (!addedToBatch) {
                List<MessageTiming> newBatch = new ArrayList<>();
                newBatch.add(timing);
                batches.add(newBatch);
            }
        }
        
        return batches;
    }
    
    private static void printPartitionBatches(int partition, List<List<MessageTiming>> batches) {
        System.out.println("Partition " + partition + ":");
        for (int i = 0; i < batches.size(); i++) {
            System.out.println("  Batch " + (i + 1) + ": " + batches.get(i).size() + " message(s)");
        }
        System.out.println();
    }
}

