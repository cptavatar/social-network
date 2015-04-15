#!/bin/sh
DATA_DIR=/Users/rosea/Projects/social-network/docker/data
rm -rf data/*
docker run  -t -p 7474:7474 --cap-add=SYS_RESOURCE -v $DATA_DIR:/var/lib/neo4j/data/graph.db neo4j
