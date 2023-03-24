FROM flink:1.10.0-scala_2.12

# Create a directory to hold the JAR files
RUN mkdir /job

# Copy the JAR files into the container
COPY pipeline/denormalizer/target/denormalizer-1.0.0.jar /job/denormalizer-1.0.0.jar
COPY pipeline/extractor/target/extractor-1.0.0.jar /job/extractor-1.0.0.jar
COPY pipeline/preprocessor/target/preprocessor-1.0.0.jar /job/preprocessor-1.0.0.jar
copy pipeline/druid-router/target/druid-router-1.0.0.jar /job/druid-router-1.0.0.jar
copy pipeline/transformer/target/transformer-1.0.0.jar   /job/transformer-1.0.0.jar
copy pipeline/pipeline-merged/target/pipeline-merged-1.0.0.jar /job/pipeline-merged-1.0.0.jar


CMD ["flink", "run", "-m", "jobmanager:8081", "/job/denormalizer-1.0.0.jar"]
CMD ["flink", "run", "-m", "jobmanager:8081", "/job/extractor-1.0.0.jar"]
CMD ["flink", "run", "-m", "jobmanager:8081", "/job/preprocessor-1.0.0.jar"]
CMD ["flink", "run", "-m", "jobmanager:8081", "/job/transformer-1.0.0.jar"]
CMD ["flink", "run", "-m", "jobmanager:8081", "/job/pipeline-merged-1.0.0.jar"]

