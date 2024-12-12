# Kafka-Spark-Cassandra Expense Tracker

This project demonstrates an end-to-end streaming application to generate, process, and store expense data using Kafka, Spark, and Cassandra. It also integrates with PostgreSQL for user data management.

## Overview

1. **Data Generator**:
   - Generates random expense data for users fetched from a PostgreSQL database.
   - Publishes the expense data to Kafka topics.

2. **Stream Handler**:
   - Consumes Kafka streams.
   - Processes and writes the expense data to a Cassandra database.

## Project Setup

### Prerequisites

Ensure the following are installed and properly configured on your Ubuntu system:

- Java 8 
- Apache Kafka
- Apache Spark (version 2.4.5)
- Apache Cassandra
- PostgreSQL
- sbt (Scala Build Tool)


## Getting Started

### insert emp.csv and dept.csv files in your postgre db

https://github.com/ozmen54/SWE307-2023/tree/main/Pro1/data

[CSV files](https://github.com/ozmen54/SWE307-2023/tree/main/Pro1/data)
### 1. Start PostgreSQL

Ensure PostgreSQL is running and contains a table named `emp` with user data.

```bash
sudo service postgresql start
```

### 2. Start Cassandra

Start the Cassandra server:

```bash
./bin/cassandra -f
```

### 3. Start Kafka and Zookeeper

Start Zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

### 4. Create Kafka Topic

Create a topic to publish expense data:

```bash
bin/kafka-topics.sh --create --topic user-expenses --bootstrap-server localhost:9092
```

### 5. Run Data Generator

Use the `data_generator.py` script to generate and publish expense data:

```bash
python3 data_generator.py
```

### 6. Verify Kafka Data

Consume messages from Kafka to verify the data:

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --include "user_.*_expenses" --from-beginning
```

### 7. Build and Run Stream Handler

1. Package the project using sbt:

   ```bash
   sbt package
   ```

2. Submit the Spark job:

   ```bash
   spark-submit --class StreamHandler \
   --master local[*] \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,\
   com.datastax.cassandra:cassandra-driver-core:4.0.0,\
   com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 \
   target/scala-2.11/stream-handler_2.11-1.0.jar
   ```

## Key Commands Summary

```bash
# Start PostgreSQL
sudo service postgresql start

# Start Cassandra
./bin/cassandra -f

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties

# Create Kafka Topic
bin/kafka-topics.sh --create --topic user-expenses --bootstrap-server localhost:9092

# Consume Kafka Messages
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --include "user_.*_expenses" --from-beginning

# Package with sbt
sbt package

# Submit Spark Job
spark-submit --class StreamHandler \
--master local[*] \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,\
com.datastax.cassandra:cassandra-driver-core:4.0.0,\
com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 \
target/scala-2.11/stream-handler_2.11-1.0.jar
```

## Project Repository

This project is part of the [Kafka-Spark-Cassandra-Expense-Tracker](https://github.com/FatihArslan-cmd/Kafka-Spark-Cassandra-Expense-Tracker) repository.

## License

This project is open-source and available under the MIT License.
