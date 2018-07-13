#!/bin/sh
mvn install:install-file -Dfile=$(pwd)/target/jedis-3.0.0-SNAPSHOT.jar \
-DgroupId=redis.clients -DartifactId=jedis -Dversion=3.0.0 \
-Dpackaging=jar -DgeneratePom=true
