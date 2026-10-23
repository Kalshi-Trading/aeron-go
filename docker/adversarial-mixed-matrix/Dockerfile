# Java side of systests/adversarial/mixed-matrix: compiles MatrixPeer against aeron-all 1.53.3.
FROM eclipse-temurin:21-jdk AS build
ARG AERON_VERSION=1.53.3
ADD https://maven-central.storage-download.googleapis.com/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar /opt/aeron/aeron-all.jar
COPY src /src
RUN mkdir /classes && javac -cp /opt/aeron/aeron-all.jar -d /classes /src/MatrixPeer.java \
    && jar cf /opt/aeron/matrix-peer.jar -C /classes .

FROM eclipse-temurin:21-jre
COPY --from=build /opt/aeron /opt/aeron
RUN chmod 755 /opt/aeron && chmod 644 /opt/aeron/*.jar
ENV JAVA_OPTS="--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -cp /opt/aeron/aeron-all.jar:/opt/aeron/matrix-peer.jar MatrixPeer \"$@\"", "--"]
