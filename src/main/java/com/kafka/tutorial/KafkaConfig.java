package com.kafka.tutorial;

/**
 * Configuration constants for Kafka Producer and Consumer
 * This class centralizes all default configuration values
 */
public class KafkaConfig {
    
    // Bootstrap server address for OpenShift Kafka cluster
    public static final String BOOTSTRAP_SERVERS = "kafka-kafka-bootstrap.kafka-user1.svc.cluster.local:9092";
    
    // Default topic name for the tutorial
    public static final String DEFAULT_TOPIC = "tutorial-topic";
    
    // Producer default configurations
    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String ACKS = "all"; // Wait for all replicas to acknowledge
    
    // Producer demo parameters (configurable)
    public static final int DEFAULT_BATCH_SIZE = 16384; // 16 KB (default)
    public static final int DEFAULT_LINGER_MS = 0; // Send immediately (default)
    
    // Consumer default configurations
    public static final String GROUP_ID = "tutorial-consumer-group";
    public static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final boolean ENABLE_AUTO_COMMIT = true;
    
    // Consumer demo parameters (configurable)
    public static final String DEFAULT_AUTO_OFFSET_RESET = "latest"; // "earliest" or "latest"
    public static final int DEFAULT_FETCH_MIN_BYTES = 1; // Default: return immediately (1 byte minimum)
    public static final int DEFAULT_FETCH_MAX_WAIT_MS = 500; // Default: wait up to 500ms
    
    // Number of sample messages to send/receive
    public static final int SAMPLE_MESSAGE_COUNT = 10;
}

