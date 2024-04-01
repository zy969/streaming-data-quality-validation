# Streaming Data Quality Validation

## Overview

This project aims to validate the quality of streaming data in real-time, using NYC Taxi Rides datasets as a case study. It leverages the power of Kafka for streaming, Flink for data processing, and [StreamDQ](https://github.com/stefan-grafberger/StreamDQ) for data quality validation, ensuring high-quality training data for modern data-driven applications.

## Requirements

- Docker
- Docker-compose
- Python 3
- Java 11
- Maven
- [StreamDQ](https://github.com/stefan-grafberger/StreamDQ) 


## Framework
- Google Cloud Storage (for data storage)
- Kafka (for streaming)
- Flink (for processing)
- [StreamDQ](https://github.com/stefan-grafberger/StreamDQ) (for data quality validation)

![Workflow](workflow.png)

## Datasets

Our experiments rely on the NYC green taxi trip records datasets spanning 2019 to 2023. These datasets provide a comprehensive view of taxi activities in New York City, uncovering travel patterns, fare insights, and service usage. We automate the download and upload of these datasets to Google Cloud Storage using the script [`upload-file-to-gcp.py`](https://github.com/zy969/streaming-data-quality-validation/blob/main/scripts/upload-file-to-gcp.py).

## Usage

To ensure a smooth execution, please make sure all the requirements listed in the [Requirements](#requirements) section are properly installed on your system.

1. Clone the repository:
    ```bash
    git clone https://github.com/zy969/streaming-data-quality-validation.git
    ```

2. Replace the local Maven `streamdq` JAR with our modified version from [`src/main/resources/lib/streamdq-1.0-SNAPSHOT.jar`](https://github.com/zy969/streaming-data-quality-validation/blob/main/src/main/resources/lib/streamdq-1.0-SNAPSHOT.jar).

3. Build the Docker image and run Docker Containers:
    ```bash
    ./scripts/build-and-run.sh
    ```

4. To monitor the logs of the running containers:
    ```bash
    docker-compose logs
    ```

5. To stop and remove containers:
    ```bash
    docker-compose down
    ```

## Troubleshooting

- **Java Version Issue**: Run `mvn --version` to ensure your Java version is 11.

- **Bash Script Issue**: If encountering bash script errors, ensure the script uses LF (Unix) line endings instead of CRLF (Windows). Use a text editor or `dos2unix` to convert the line endings.





