FROM eclipse-temurin:21-jdk AS build
ARG AERON_VERSION=1.53.3
ADD https://maven-central.storage-download.googleapis.com/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar /opt/aeron/aeron-all.jar
COPY ChallengeAuthenticatorSupplier.java /src/
RUN mkdir /opt/auth && javac -cp /opt/aeron/aeron-all.jar -d /opt/auth /src/ChallengeAuthenticatorSupplier.java

FROM eclipse-temurin:21-jre
COPY --from=build /opt/aeron /opt/aeron
COPY --from=build /opt/auth /opt/auth
RUN chmod -R a+rX /opt/aeron /opt/auth
ENV JAVA_OPTS="--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -XX:+UnlockDiagnosticVMOptions -XX:GuaranteedSafepointInterval=300000"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS $AERON_PROPS -cp /opt/aeron/aeron-all.jar:/opt/auth \"$@\"", "--"]
