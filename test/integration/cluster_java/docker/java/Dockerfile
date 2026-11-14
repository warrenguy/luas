# syntax=docker/dockerfile:1
# make java-jars builds bin/luas-cluster-java.jar before this image.
FROM eclipse-temurin:26-jre

WORKDIR /app
COPY bin/luas-cluster-java.jar ./luas-cluster-java.jar
COPY --chmod=+x test/integration/cluster_java/docker/java/entrypoint-cluster.sh ./entrypoint-cluster.sh

ENTRYPOINT ["./entrypoint-cluster.sh"]
