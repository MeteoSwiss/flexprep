#!/bin/bash

echo Notification received

# Retrieve ECR login password and log in to Docker
aws ecr get-login-password --region eu-central-2 | docker login --username AWS --password-stdin 493666016161.dkr.ecr.eu-central-2.amazonaws.com
if [ $? -ne 0 ]; then
  echo "Docker login failed. Exiting."
  exit 1
fi

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --date)
    DATE="$2"
    shift # past argument
    shift # past value
    ;;
    --location)
    LOCATION="$2"
    shift # past argument
    shift # past value
    ;;
    --time)
    TIME="$2"
    shift # past argument
    shift # past value
    ;;
    --step)
    STEP="$2"
    shift # past argument
    shift # past value
    ;;
esac
done

echo Notification received for file $LOCATION, date $DATE, time $TIME, step $STEP

# Run the Docker container
# Note: The tag of the Docker image (after the colon ':') is manually updated here
# after each push to reflect the latest version of the image.
docker run \
  --mount type=bind,source="$HOME/.sqlite/",destination=/src/db/ \
  --env-file .env \
  493666016161.dkr.ecr.eu-central-2.amazonaws.com/numericalweatherpredictions/flexpart_ifs/flexprep:2409.ee22f6c67c86b9f85185edb02924e6ab523fa0bc \
  --step "$STEP" \
  --date "$DATE" \
  --time "$TIME" \
  --location "$LOCATION"

# Check if the Docker run was successful
if [ $? -ne 0 ]; then
  echo "Docker run failed."
  exit 1
fi

echo "Docker container executed successfully."
