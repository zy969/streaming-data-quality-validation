#!/bin/bash

# Step 1: Package the project with Maven
echo "Starting Maven clean and package..."
mvn clean package -X &&

# Step 2: Build the Docker image 
(echo "Building Docker image..." &&
docker build --no-cache -t data-quality-validation .) &&

# Push the Docker image to the repository
(echo "Pushing Docker image to repository..." &&
docker tag data-quality-validation:latest vic033/data-quality-validation:latest &&
docker push vic033/data-quality-validation:latest) &&

# Step 3: Start the containers 
(echo "Starting Docker containers..." &&
docker-compose up -d) &&

echo "Build-and-run completed！"
