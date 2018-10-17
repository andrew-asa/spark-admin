#! /bin/sh
dst=".."
mvn install && cp target/spark-admin-1.0-SNAPSHOT.jar "$dst/lib/spark-admin-1.0-SNAPSHOT.jar" && cp target/spark-admin-1.0-SNAPSHOT-sources.jar "$dst/lib/spark-admin-1.0-SNAPSHOT-sources.jar"