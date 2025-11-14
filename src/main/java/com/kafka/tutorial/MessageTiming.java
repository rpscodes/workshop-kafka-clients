package com.kafka.tutorial;

/**
 * Helper class to track message timing for batching analysis
 */
public class MessageTiming {
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

