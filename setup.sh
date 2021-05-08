#!/bin/sh

docker build . --tag airflow:TR
docker compose up -d
