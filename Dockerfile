FROM --platform=linux/x86_64 maven:3.6.0-jdk-11-slim AS build
COPY . /app
RUN mvn -f /app/pom.xml clean package -DskipTests

FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-java11
USER flink
COPY --from=build /app/dataset-registry/target/dataset-registry-1.0.0.jar $FLINK_HOME/lib/
COPY --from=build /app/framework/target/framework-1.0.0.jar $FLINK_HOME/lib/
COPY --from=build /app/pipeline/denormalizer/target/denormalizer-1.0.0.jar $FLINK_HOME/lib/
COPY --from=build /app/pipeline/druid-router/target/druid-router-1.0.0.jar $FLINK_HOME/lib/
COPY --from=build /app/pipeline/extractor/target/extractor-1.0.0.jar $FLINK_HOME/lib/
COPY --from=build /app/pipeline/pipeline-merged/target/pipeline-merged-1.0.0.jar $FLINK_HOME/lib/
COPY --from=build /app/pipeline/preprocessor/target/preprocessor-1.0.0.jar $FLINK_HOME/lib/
COPY --from=build /app/pipeline/transformer/target/transformer-1.0.0.jar $FLINK_HOME/lib/
