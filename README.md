# Dibimbing, Data Engineering Bootcamp

1. Clone This Repo.
2. Run `make docker-build` 
3. Run `make postgres` 
4. Run `make kafka` 
5. Run `make spark`
6. Run `make spark-produce`
7. Run `make spark-consume`

---
```
## docker-build                 - Build Docker Images (amd64) including its inter-container network.
## postgres                     - Run a Postgres container
## spark                        - Run a Spark cluster, rebuild the postgres container, then create the destination tables
## kafka                        - Spinup kafka cluster (Kafka+Zookeeper).
## clean                        - Cleanup all running containers related to the challenge.
```

---