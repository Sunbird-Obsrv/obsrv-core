FROM --platform=linux/x86_64 maven:3.9.4-eclipse-temurin-11-focal AS build-core
COPY . /app
RUN mvn clean install -DskipTests -f /app/framework/pom.xml
RUN mvn clean install -DskipTests -f /app/dataset-registry/pom.xml

FROM --platform=linux/x86_64 maven:3.9.4-eclipse-temurin-11-focal AS build-pipeline
COPY --from=build-core /root/.m2 /root/.m2
COPY . /app
RUN mvn clean package -DskipTests -f /app/pipeline/pom.xml

FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-jdk-11 as extractor-image
USER flink
COPY --from=build-pipeline /app/pipeline/extractor/target/extractor-1.0.0.jar $FLINK_HOME/lib/

FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-jdk-11 as preprocessor-image
USER flink
COPY --from=build-pipeline /app/pipeline/preprocessor/target/preprocessor-1.0.0.jar $FLINK_HOME/lib/

FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-jdk-11 as denormalizer-image
USER flink
COPY --from=build-pipeline /app/pipeline/denormalizer/target/denormalizer-1.0.0.jar $FLINK_HOME/lib/

FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-jdk-11 as transformer-image
USER flink
COPY --from=build-pipeline /app/pipeline/transformer/target/transformer-1.0.0.jar $FLINK_HOME/lib/

FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-jdk-11 as router-image
USER flink
COPY --from=build-pipeline /app/pipeline/druid-router/target/druid-router-1.0.0.jar $FLINK_HOME/lib/

FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-jdk-11 as merged-image
USER flink
COPY --from=build-pipeline /app/pipeline/pipeline-merged/target/pipeline-merged-1.0.0.jar $FLINK_HOME/lib/

FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-jdk-11 as master-data-processor-image
USER flink
COPY --from=build-pipeline /app/pipeline/master-data-processor/target/master-data-processor-1.0.0.jar $FLINK_HOME/lib

FROM --platform=linux/x86_64 sanketikahub/flink:1.15.2-scala_2.12-jdk-11 as kafka-connector-image
USER flink
COPY --from=build-pipeline /app/pipeline/kafka-connector/target/kafka-connector-1.0.0.jar $FLINK_HOME/lib