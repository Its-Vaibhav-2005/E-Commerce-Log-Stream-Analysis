# Kafka Container

- **Creating Docker Container:** Create file `docker-compose.yml` and write data
- **Start Container:**
```bash
docker compose up -d
```
- **Open kafka bash:**
```bash
docker exec -it kafka bash
```
- **Redirect to Kafka:**
```bash
cd /opt/kafka/bin
```
- **Shutdown Container**
```bash
docker compose down
```