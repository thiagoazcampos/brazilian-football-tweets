kafka-topics.sh --create --topic tweets-raw --bootstrap-server kafka:9092 --partitions 3
kafka-topics.sh --create --topic tweets-final --bootstrap-server kafka:9092 --partitions 3