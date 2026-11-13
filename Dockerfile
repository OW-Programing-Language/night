# Sample Night application image, not a certified production release.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/OW-Programing-Language/ow-lang.git ow-lang \
    && cd ow-lang \
    && git checkout 3be777a7ee2c9204ebe653d89e749e0dcfe4b831
COPY platform ./platform
COPY src ./src
COPY examples ./examples
RUN mvn -B -q -f ow-lang/pom.xml -DskipTests install \
    && mvn -B -q -f platform/pom.xml -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/ow-lang/target/OwIDE.jar /app/ow-lang/target/OwIDE.jar
COPY --from=build /workspace/platform/target/night-platform-0.2.0-SNAPSHOT.jar /app/platform/target/night-platform-0.2.0-SNAPSHOT.jar
COPY --from=build /workspace/examples /app/examples
COPY --from=build /workspace/src /app/src
ENV OW_NO_BROWSER=1
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-cp", "/app/ow-lang/target/OwIDE.jar:/app/platform/target/night-platform-0.2.0-SNAPSHOT.jar", "dev.oreo.Main", "run", "examples/production/main.ow"]
