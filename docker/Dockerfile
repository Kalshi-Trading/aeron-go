# JAVA_IMAGE can point at a registry mirror (e.g. mirror.gcr.io/library/eclipse-temurin:21-jre) when Docker Hub rate limits.
ARG JAVA_IMAGE=eclipse-temurin:21-jre
FROM ${JAVA_IMAGE}
ARG AERON_VERSION=1.53.3
ADD https://maven-central.storage-download.googleapis.com/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar /opt/aeron/aeron-all.jar
RUN chmod 755 /opt/aeron && chmod 644 /opt/aeron/aeron-all.jar
ENV JAVA_OPTS="--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -XX:+UnlockDiagnosticVMOptions -XX:GuaranteedSafepointInterval=300000"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS $AERON_PROPS -cp /opt/aeron/aeron-all.jar \"$@\"", "--"]
