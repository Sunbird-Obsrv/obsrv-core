FROM sanketikahub/flink:1.15.2-scala_2.12-jdk-11-source
USER flink
RUN mkdir $FLINK_HOME/plugins/s3-fs-presto
RUN mkdir $FLINK_HOME/plugins/gs-fs-hadoop
RUN wget -nv -O flink-streaming-scala_2.12-1.15.2.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-streaming-scala_2.12/1.15.2/flink-streaming-scala_2.12-1.15.2.jar"; \ 
    mv flink-streaming-scala_2.12-1.15.2.jar $FLINK_HOME/lib/
# COPY flink-shaded-hadoop2-uber-2.8.3-1.8.3.jar $FLINK_HOME/lib/
# COPY flink-s3-fs-hadoop-1.15.2.jar $FLINK_HOME/lib/
RUN wget -nv -O flink-azure-fs-hadoop-1.15.2.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-azure-fs-hadoop/1.15.2/flink-azure-fs-hadoop-1.15.2.jar"; \ 
    mv flink-azure-fs-hadoop-1.15.2.jar $FLINK_HOME/lib/
RUN wget -nv -O flink-s3-fs-presto-1.15.2.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-presto/1.15.2/flink-s3-fs-presto-1.15.2.jar"; \ 
    mv flink-s3-fs-presto-1.15.2.jar $FLINK_HOME/plugins/s3-fs-presto
RUN wget -nv -O flink-gs-fs-hadoop-1.15.2.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-gs-fs-hadoop/1.15.2/flink-gs-fs-hadoop-1.15.2.jar"; \ 
    mv flink-gs-fs-hadoop-1.15.2.jar $FLINK_HOME/plugins/gs-fs-hadoop
