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

    docker tag data-quality-validation:latest vic033/data-quality-validation:latest

    docker push vic033/data-quality-validation:latest


4. Run Docker Containers:
    ```bash
    docker-compose up -d
    ```

5. To monitor the logs of the running containers:
    ```bash
    docker-compose logs
    ```
查看容器：
docker ps

查看consumer：
docker logs streaming-data-quality-validation-consumer-1


6. To track the real-time logs of the running consumer:
    ```bash
    docker-compose logs -f consumer
    ```

7. To stop and remove containers:
    ```bash
    docker-compose down
    ```

(移除所有镜像)
    ```bash
    docker rmi $(docker images -q)
    ```


## 数据结构：
//        green_tripdata.parquet
//        VendorID int32
//        lpep_pickup_datetime datetime64[us]
//        lpep_dropoff_datetime datetime64[us]
//        store_and_fwd_flag object
//        RatecodeID float64
//        PULocationID int32
//        DOLocationID int32
//        passenger_count float64
//        trip_distance float64
//        fare_amount float64
//        extra float64
//        mta_tax float64
//        tip_amount float64
//        tolls_amount float64
//        ehail_fee float64
//        improvement_surcharge float64
//        total_amount float64
//        payment_type float64
//        trip_type float64
//        congestion_surcharge float64


## Change logs

- 3.14 修改了适合green_tripdata.parquet的Producer中的convertGroupToJson方法和发送每条record会间隔1000ms来模拟数据流









