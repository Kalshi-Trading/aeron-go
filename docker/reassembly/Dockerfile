FROM eclipse-temurin:21-jdk AS build
ARG AERON_VERSION=1.53.3
ADD https://maven-central.storage-download.googleapis.com/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar /opt/aeron/aeron-all.jar
COPY LargeMessagePublisher.java /src/
RUN javac -cp /opt/aeron/aeron-all.jar -d /opt/aeron/classes /src/LargeMessagePublisher.java

FROM eclipse-temurin:21-jre
COPY --from=build /opt/aeron /opt/aeron
RUN chmod -R a+rX /opt/aeron
ENV JAVA_OPTS="--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS $AERON_PROPS -cp /opt/aeron/aeron-all.jar:/opt/aeron/classes LargeMessagePublisher \"$@\"", "--"]
