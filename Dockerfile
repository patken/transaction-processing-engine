FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
COPY --from=build /workspace/target/*.jar app.jar
USER app
EXPOSE 8080

# Cgroup-aware heap sizing: the JVM is container-aware by default on 21, but the stock
# MaxRAMPercentage of 25% wastes most of a small container's memory limit. 75% leaves
# headroom for thread stacks, metaspace, and direct buffers (Kafka/Netty) off-heap.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0"

# Uses the open /actuator/health endpoint; the schedulers/kafka/db indicators make this a
# real readiness signal, not just "process alive". busybox wget ships in the alpine base.
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD wget --spider -q http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
