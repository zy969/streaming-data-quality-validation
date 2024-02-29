# Stage 1: Build Stage
FROM maven:3.8.4-openjdk-8 AS build

# Copy Maven project files
WORKDIR /app
COPY pom.xml .
COPY src ./src
COPY dataset ./dataset 

# Build the project
RUN mvn clean package
COPY wait-for-it.sh /app/
RUN chmod +x wait-for-it.sh

# Stage 2: Production Stage
FROM openjdk:8u151-jdk-alpine3.7

# Install required packages
RUN apk add --no-cache bash libc6-compat

# Set working directory
WORKDIR /

# Copy built JAR and script files
COPY --from=build /app/target/data-quality-validation-1.0-jar-with-dependencies.jar data-quality-validation-1.0.jar
COPY --from=build /app/wait-for-it.sh .
COPY --from=build /app/dataset ./dataset 

# Define the entrypoint command
CMD ./wait-for-it.sh $ZOOKEEPER_SERVER && ./wait-for-it.sh $KAFKA_SERVER && java -Xmx512m -jar data-quality-validation-1.0.jar
