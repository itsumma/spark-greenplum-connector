# Building ITSumma Spark Greenplum Connector

This library can be built using either Maven or Gradle, Maven is recommended as we use it during development.  
Currently the resulting jar is targeted to Java version 1.8, thus you need it for build process,  
e.g. install openjdk version 1.8 on your machine and define JAVA_HOME environment variable pointing to JVM directory:  
`export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre`  
Download latest release source code or checkout it from github repository according to your Spark version:
 - v2.x.x branches of connector correspond to Spark 2.4 (or up) installation
 - v3.x.x branches of connector correspond to Spark 3.4 (or up) installation

Then go to the source tree root to start a build.
## Maven
Apache Maven 3.x.x is required. As for now, we use v3.6.3 from Ubuntu 22.04 distribution.  
Install it and run:
```agsl
mvn clean package
```
Look for resulting JAR in the spark-greenplum-connector/target/ folder.
Also there is `install` target which will copy the resulting file into /home/hadoop/spark/jars/ directory.

## Gradle
First you will need to install Gradle 6.x version suitable for your OS somehow. Google for instructions.  
Second you'l have to build the gradlew wrapper script which actually builds the project:
```agsl
gradle wrapper --gradle-version 6.3
```
After you have a **gradlew** script in your source tree root folder, run:
```bash
./gradlew shadowJar
```
The result should appear in the spark-greenplum-connector/build/libs/ folder.
