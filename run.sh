#!/bin/bash

# Kafka Java Tutorial - Run Script
# This script builds and runs the Kafka tutorial application

set -e

echo "========================================"
echo "   Kafka Java Tutorial Application"
echo "========================================"
echo ""

# Check if Java is installed
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed or not in PATH"
    echo "Please install Java 17 or higher"
    exit 1
fi

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed or not in PATH"
    echo "Please install Maven 3.6 or higher"
    exit 1
fi

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 17 ]; then
    echo "Error: Java 17 or higher is required. Found Java $JAVA_VERSION"
    exit 1
fi

echo "Building the project..."
mvn clean compile -q

echo "Running the application..."
echo ""

# Run using Maven exec plugin
mvn exec:java -Dexec.mainClass="com.kafka.tutorial.KafkaTutorialApp"

