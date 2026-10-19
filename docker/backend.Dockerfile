# syntax=docker/dockerfile:1

# Compile once on the builder's architecture: the resulting Java 8 bytecode is
# shared by both runtime architectures. No production environment is needed.
FROM --platform=$BUILDPLATFORM maven:3.9-eclipse-temurin-8 AS build
WORKDIR /workspace
COPY .mvn/settings.xml .mvn/settings.xml
COPY hoj-springboot/pom.xml hoj-springboot/pom.xml
COPY hoj-springboot/api/pom.xml hoj-springboot/api/pom.xml
COPY hoj-springboot/DataBackup/pom.xml hoj-springboot/DataBackup/pom.xml
COPY hoj-springboot/JudgeServer/pom.xml hoj-springboot/JudgeServer/pom.xml
COPY hoj-springboot/api/src hoj-springboot/api/src
COPY hoj-springboot/DataBackup/src hoj-springboot/DataBackup/src
COPY hoj-springboot/JudgeServer/src hoj-springboot/JudgeServer/src
COPY docker/backend-tests.txt /workspace/backend-tests.txt
RUN --mount=type=cache,id=sxuoj-maven,target=/root/.m2,sharing=locked \
    set -eu; \
    tests="$(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' backend-tests.txt | tr -d '\r' | paste -sd, -)"; \
    test -n "$tests"; \
    mvn -B -ntp -s .mvn/settings.xml -f hoj-springboot/pom.xml package \
        -DskipTests=false -Dmaven.test.skip=false -Dmaven.test.failure.ignore=false \
        "-Dtest=$tests" -Dsurefire.failIfNoSpecifiedTests=false; \
    mkdir /out; \
    cp hoj-springboot/DataBackup/target/hoj-backend-*.jar /out/backend.jar; \
    cp hoj-springboot/JudgeServer/target/hoj-judgeServer-*.jar /out/judgeserver.jar; \
    cd /out; sha256sum backend.jar judgeserver.jar > SHA256SUMS

# Export with: docker buildx build --target artifacts --output type=local,dest=dist/backend ...
# The judge jar still requires the existing judge image's compilers and sandbox.
FROM scratch AS artifacts
COPY --from=build /out/ /

FROM eclipse-temurin:8-jre-jammy AS backend
LABEL io.sxuoj.component="backend"
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl fontconfig fonts-dejavu-core fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/hoj /hoj/file /hoj/testcase /hoj/log/backend
WORKDIR /hoj
ENV SPRING_PROFILES_ACTIVE=prod \
    TZ=Asia/Shanghai
# Keep executables outside /hoj: legacy deployments often mount that directory.
COPY --from=build /out/backend.jar /opt/hoj/backend.jar
COPY --chmod=755 docker/backend-entrypoint.sh /usr/local/bin/hoj-backend
COPY --chmod=755 docker/backend-healthcheck.sh /usr/local/bin/hoj-healthcheck
EXPOSE 6688
HEALTHCHECK --interval=15s --timeout=6s --start-period=120s --retries=8 \
    CMD ["/usr/local/bin/hoj-healthcheck"]
# Root preserves compatibility with existing bind-mount owners. A deployment may
# override user after assigning write permissions to the data/log directories.
ENTRYPOINT ["/usr/local/bin/hoj-backend"]
