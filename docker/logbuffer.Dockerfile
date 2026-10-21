# JDK image so LogMetaDump.java can be run with the single-file source launcher next to LogInspector.
FROM eclipse-temurin:21-jdk
ARG AERON_VERSION=1.53.3
ADD https://maven-central.storage-download.googleapis.com/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar /opt/aeron/aeron-all.jar
COPY logbuffer/LogMetaDump.java /opt/aeron/LogMetaDump.java
RUN chmod 755 /opt/aeron && chmod 644 /opt/aeron/aeron-all.jar /opt/aeron/LogMetaDump.java
ENV JAVA_OPTS="--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -cp /opt/aeron/aeron-all.jar \"$@\"", "--"]
