# JDK image (needs javac) that compiles PubVerifier against aeron-all 1.53.3. Uses the GCR Docker Hub mirror to
# avoid Docker Hub pull rate limits.
FROM mirror.gcr.io/library/eclipse-temurin:21-jdk
ARG AERON_VERSION=1.53.3
ADD https://maven-central.storage-download.googleapis.com/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar /opt/aeron/aeron-all.jar
RUN chmod 755 /opt/aeron && chmod 644 /opt/aeron/aeron-all.jar
COPY PubVerifier.java /opt/verifier/
RUN javac -cp /opt/aeron/aeron-all.jar -d /opt/verifier /opt/verifier/PubVerifier.java && chmod -R a+rX /opt/verifier
ENV JAVA_OPTS="--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS $AERON_PROPS -cp /opt/aeron/aeron-all.jar:/opt/verifier PubVerifier"]
