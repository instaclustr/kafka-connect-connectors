#!/bin/sh

aws s3 cp /usr/src/kafka-connect-connectors/target/distribution-0.1.3-uber.jar s3://$AWS_BUCKET/