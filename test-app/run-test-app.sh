## To run under debugger add:
#       --conf spark.driver.extraJavaOptions="-Djava.net.preferIPv4Stack=True -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=localhost:5005" \
#       --conf spark.executor.heartbeatInterval=1h \
#       --conf spark.network.timeout=12h \

## Example for run spark-greenplum-connector test application
## Pass the number of rows to generate/insert as argument for this script
log4j_setting="-Dlog4j.configuration=file:log4j.properties"

spark-submit --master yarn --deploy-mode client \
        --num-executors 2 \
        --executor-cores 1 \
        --executor-memory 1G \
        --driver-memory 1G \
        --files "./log4j.properties" \
        --conf "spark.driver.extraJavaOptions=${log4j_setting}" \
        --conf "spark.executor.extraJavaOptions=${log4j_setting}" \
        --conf spark.default.parallelism=10 \
        --conf spark.task.cpus=1 \
        --conf spark.greenplum.jdbc.url=<jdbc:postgresql://host:port/database> \
        --conf spark.greenplum.user=<dbuser> \
        --conf spark.greenplum.password=<dbpassword> \
        --conf spark.greenplum.dbtable=<tablename> \
        --conf spark.greenplum.mode=<overwrite|append> \
        --conf spark.greenplum.buffer.size=20000 \
        --class com.itsumma.gpconnector.testapp.ItsGpConnectorTestApp \
        test-app_2.11-2.1.jar \
        $@
