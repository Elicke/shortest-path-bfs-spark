#!/bin/sh

/usr/lib/jvm/java-1.8.0/bin/javac -cp $HOME/Spark/jars/spark-core_2.11-2.3.1.jar:$HOME/Spark/jars/spark-sql_2.11-2.3.1.jar:$HOME/Spark/jars/scala-library-2.11.8.jar:$HOME/Spark/google-collections-1.0.jar:$HOME/Spark/jars/log4j-1.2.17.jar *.java
/usr/lib/jvm/java-1.8.0/bin/jar -cvf ShortestPath.jar ShortestPath.class Data.class
