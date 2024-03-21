# Use base image
FROM openjdk:8u151-jdk-alpine3.7

# Install necessary packages
RUN apk add --no-cache bash libc6-compat

# Set working directory
WORKDIR /

# Copy build artifacts and other necessary files
COPY target/data-quality-validation-1.0-jar-with-dependencies.jar data-quality-validation-1.0.jar
COPY scripts/wait-for-it.sh .
COPY key.json /
COPY src/main/resources/log4j2.xml /

# Grant execution permissions to scripts
RUN chmod +x wait-for-it.sh

# Define startup command
CMD ./wait-for-it.sh $ZOOKEEPER_SERVER && ./wait-for-it.sh $KAFKA_SERVER && java -Xmx512m -jar data-quality-validation-1.0.jar
