#!/bin/sh
# Simple script to update neo4j with our config files
PROJECT_HOME=~/Projects/social-network
NEO4J_HOME=/usr/local/Cellar/neo4j
VERSION=2.1.5
cd $PROJECT_HOME/conf/neo4j && cp * $NEO4J_HOME/$VERSION/libexec/conf/
