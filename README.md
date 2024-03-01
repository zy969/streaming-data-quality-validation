# Streaming Data Quality Validation

## Requirements

- Python 3
- Java 11
- Maven
- Docker
- [StreamDQ](https://github.com/stefan-grafberger/StreamDQ)

## Setup

1. Clone the repository:
    ```bash
    git clone https://github.com/zy969/streaming-data-quality-validation.git
    ```

2. Download Datasets:
    ```bash
    python download_datasets.py
    ```

3. Build the Docker image:
    ```bash
    docker build --no-cache -t data-quality-validation .
    ```

docker tag data-quality-validation:latest vic033/data-quality-validation:latest

docker push vic033/data-quality-validation:latest


## Usage


1. Run Docker Containers:
    ```bash
    docker-compose up -d
    ```

2. To check running containers:
    ```bash
    docker ps
    ```

3. To stop and remove containers:
    ```bash
    docker-compose down
    ```




docker-compose stop
docker rmi $(docker images -q)

docker logs 1-kafka-1
docker logs 1-producer-1
docker logs 1-consumer-1  
docker logs 1-zookeeper-1
docker logs 1-manager-1  






