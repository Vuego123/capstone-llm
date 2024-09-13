#!/usr/bin/env bash
docker run -d -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN capstone:mattias python3 -m capstonellm.tasks.clean -e local -t airflow
sleep 60
docker run -d -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN capstone:mattias python3 -m capstonellm.tasks.clean -e local -t apache-spark
sleep 60
docker run -d -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN capstone:mattias python3 -m capstonellm.tasks.clean -e local -t dbt
sleep 60
docker run -d -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN capstone:mattias python3 -m capstonellm.tasks.clean -e local -t docker
sleep 60
docker run -d -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN capstone:mattias python3 -m capstonellm.tasks.clean -e local -t pyspark
sleep 60
docker run -d -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN capstone:mattias python3 -m capstonellm.tasks.clean -e local -t python-polars
sleep 60
docker run -d -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN capstone:mattias python3 -m capstonellm.tasks.clean -e local -t sql