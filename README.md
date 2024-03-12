# Streaming Data Quality Validation

## Overview

This project aims to validate the quality of streaming data in real-time, using NYC Taxi Rides datasets as a case study. It leverages the power of Kafka for streaming, Flink for data processing, and [StreamDQ](https://github.com/stefan-grafberger/StreamDQ) for data quality validation, ensuring high-quality training data for modern ML applications.

## Requirements

- Docker
- Docker-compose
- Python 3
- Java 11
- Scala
- Maven

## Framework

- Kafka (for streaming)
- Flink (for processing)
- [StreamDQ](https://github.com/stefan-grafberger/StreamDQ) (for data quality validation)


## Usage

1. Clone the repository:
    ```bash
    git clone https://github.com/zy969/streaming-data-quality-validation.git
    ```

2. Automatically download the 2023 NYC Taxi Rides datasets and upload them to Google Cloud Storage: 
    ```bash
    python upload_file_to_gcp.py 
    ```

3. Build the Docker image:
    ```bash
    docker build --no-cache -t data-quality-validation .
    ```

（更改代码后Push镜像:）
    ```bash
    docker tag data-quality-validation:latest vic033/data-quality-validation:latest
    ```
    ```bash
    docker push vic033/data-quality-validation:latest
    ```

4. Run Docker Containers:
    ```bash
    docker-compose up -d
    ```

5. To monitor the logs of the running containers:
    ```bash
    docker-compose logs
    ```

6. To stop and remove containers:
    ```bash
    docker-compose down
    ```

(移除所有镜像)
    ```bash
    docker rmi $(docker images -q)
    ```


数据结构：
{
  "type" : "record",
  "name" : "schema",
  "fields" : [ {
    "name" : "dispatching_base_num",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "pickup_datetime",
    "type" : [ "null", {
      "type" : "long",
      "logicalType" : "timestamp-micros"
    } ],
    "default" : null
  }, {
    "name" : "dropOff_datetime",
    "type" : [ "null", {
      "type" : "long",
      "logicalType" : "timestamp-micros"
    } ],
    "default" : null
  }, {
    "name" : "PUlocationID",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "DOlocationID",
    "type" : [ "null", "double" ],
    "default" : null
  }, {
    "name" : "SR_Flag",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "Affiliated_base_number",
    "type" : [ "null", "string" ],
    "default" : null
  } ]
}










