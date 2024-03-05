# Streaming Data Quality Validation

## Requirements

- Python 3
- Java 11
- Scala
- Maven
- Docker
- [StreamDQ](https://github.com/stefan-grafberger/StreamDQ)

## Setup

1. Clone the repository:
    ```bash
    git clone https://github.com/zy969/streaming-data-quality-validation.git
    ```

2. Upload Datasets: (传好了，不用运行)
    ```bash
    python  upload_file_to_gcp.py 
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

docker logs streaming-data-quality-validation-consumer-1 

3. To stop and remove containers:
    ```bash
    docker-compose down
    ```


docker rmi $(docker images -q)




parquet schema dataset/fhv_tripdata_2023-01.parquet

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










