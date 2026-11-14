# syntax=docker/dockerfile:1
# make java-jars builds bin/luas-java-interop.jar before this image.
FROM eclipse-temurin:26-jre

WORKDIR /app
COPY bin/luas-java-interop.jar ./luas-java-interop.jar
COPY --chmod=+x test/integration/java_interop/docker/java/entrypoint-java.sh ./entrypoint-java.sh

ENTRYPOINT ["./entrypoint-java.sh"]
