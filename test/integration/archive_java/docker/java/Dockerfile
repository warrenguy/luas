# syntax=docker/dockerfile:1
# make java-jars builds bin/luas-archive-java.jar before this image.
FROM eclipse-temurin:26-jre

WORKDIR /app
COPY bin/luas-archive-java.jar ./luas-archive-java.jar
COPY --chmod=+x test/integration/archive_java/docker/java/entrypoint-archive.sh ./entrypoint-archive.sh

ENTRYPOINT ["./entrypoint-archive.sh"]
