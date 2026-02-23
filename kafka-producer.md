# Kafka Topics 
- **Creating Topic:** 
```bash
./kafka-topics.sh --create --topic raw_events --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

- **Geting Topic List:**
```bash
./kafka-topics.sh --list --bootstrap-server kafka:9092
```

- **Description of Topic:**
```bash
./kafka-topics.sh --describe --topic raw_events --bootstrap-server kafka:9092
```
