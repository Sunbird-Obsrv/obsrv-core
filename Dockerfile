# FROM flink:1.10.0-scala_2.12

# # Create a directory to hold the JAR files
# USER flink

# # Copy the JAR files into the container
# COPY /pipeline/denormalizer/target/denormalizer-1.0.0.jar $FLINK_HOME/lib/denormalizer-1.0.0.jar
# COPY pipeline/extractor/target/extractor-1.0.0.jar $FLINK_HOME/lib/extractor-1.0.0.jar
# COPY pipeline/preprocessor/target/preprocessor-1.0.0.jar $FLINK_HOME/lib/preprocessor-1.0.0.jar
# copy pipeline/druid-router/target/druid-router-1.0.0.jar $FLINK_HOME/lib/druid-router-1.0.0.jar
# copy pipeline/transformer/target/transformer-1.0.0.jar   $FLINK_HOME/lib/transformer-1.0.0.jar
# copy pipeline/pipeline-merged/target/pipeline-merged-1.0.0.jar $FLINK_HOME/lib/pipeline-merged-1.0.0.jar
# Use the Flink 1.10.0 image as the base image
# FROM flink:1.10.0-scala_2.12

# # Set the user to 'flink' to avoid running as root
# USER flink

# # Create a directory to hold the JAR files
# RUN mkdir -p /opt/flink/lib

# # Copy the JAR files into the container
# COPY pipeline/denormalizer/target/denormalizer-1.0.0.jar /opt/flink/lib/denormalizer-1.0.0.jar
# COPY pipeline/extractor/target/extractor-1.0.0.jar /opt/flink/lib/extractor-1.0.0.jar
# COPY pipeline/preprocessor/target/preprocessor-1.0.0.jar /opt/flink/lib/preprocessor-1.0.0.jar
# COPY pipeline/druid-router/target/druid-router-1.0.0.jar /opt/flink/lib/druid-router-1.0.0.jar
# COPY pipeline/transformer/target/transformer-1.0.0.jar /opt/flink/lib/transformer-1.0.0.jar
# COPY pipeline/pipeline-merged/target/pipeline-merged-1.0.0.jar /opt/flink/lib/pipeline-merged-1.0.0.jar

# # Set the Flink classpath to include the JAR files
# ENV FLINK_CLASSPATH=${FLINK_HOME}/lib/*

# # Expose the Flink job manager and task manager ports
# EXPOSE 6123 8081

# # Start the Flink cluster in job manager mode
# CMD ["jobmanager"]
FROM flink:1.15.2-scala_2.12-java11
USER flink

COPY pipeline/denormalizer/target/denormalizer-1.0.0.jar $FLINK_HOME/lib/denormalizer-1.0.0.jar
COPY pipeline/extractor/target/extractor-1.0.0.jar $FLINK_HOME/lib/extractor-1.0.0.jar
COPY pipeline/preprocessor/target/preprocessor-1.0.0.jar $FLINK_HOME/lib/preprocessor-1.0.0.jar
copy pipeline/druid-router/target/druid-router-1.0.0.jar $FLINK_HOME/lib/druid-router-1.0.0.jar
copy pipeline/transformer/target/transformer-1.0.0.jar   $FLINK_HOME/lib/transformer-1.0.0.jar
copy pipeline/pipeline-merged/target/pipeline-merged-1.0.0.jar $FLINK_HOME/lib/pipeline-merged-1.0.0.jar