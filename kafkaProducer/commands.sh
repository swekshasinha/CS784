# Run these commands in confluence-kafka folder.


# Start server.
./bin/kafka-server-start ./etc/kafka/kraft/server.properties

# Create kafka topic.
./bin/kafka-topics --bootstrap-server localhost:9092 \
                   --create \
                   --topic crypto_topic

# Send message to kafka topic.
./bin/kafka-console-producer --bootstrap-server localhost:9092 \
                             --topic crypto_topic