#!/bin/sh
NEOJ=/usr/local/bin/neo4j
$NEOJ start
read -p "Press any key to stop..." -n 1 -s
$NEOJ stop
