#!/bin/sh

# spark-submit --class ShortestPath --master local ShortestPath.jar (filename) (source vertex) (destination vertex)
spark-submit --class ShortestPath --master local ShortestPath.jar $1 $2 $3

