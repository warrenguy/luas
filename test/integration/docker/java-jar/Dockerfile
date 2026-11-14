# syntax=docker/dockerfile:1
# Packages one Java module's jar. The context is the module directory, and
# the scripts context is the repo scripts directory. Builder ships Maven and
# JDK 26 to match pom maven.compiler.release.
FROM maven:3.9-eclipse-temurin-26 AS builder

ARG ARTIFACT

WORKDIR /src
COPY --from=scripts mvn-retry.sh /usr/local/bin/
COPY pom.xml ./
COPY src src

# cp fails unless the glob matches exactly one jar.
RUN --mount=type=cache,target=/root/.m2/repository,sharing=locked \
	mvn-retry.sh -q -Dmaven.test.skip=true package && \
	mkdir /out && cp target/"${ARTIFACT:?}"-*.jar "/out/${ARTIFACT}.jar"

FROM scratch
COPY --from=builder /out/ /
