#!/bin/bash
cd /home/ratings/ratings_history/
source venv/bin/activate
docker-compose up --build --no-start
