FROM sunbird/flink:1.15.2-scala_2.12-java11
COPY target/obsrv-distribution.tar.gz /tmp
USER flink
RUN tar -xvf /tmp/obsrv-distribution.tar.gz -C $FLINK_HOME/lib/
RUN mkdir $FLINK_HOME/plugins/s3-fs-presto
RUN cp $FLINK_HOME/opt/flink-s3-fs-presto-1.15.2.jar $FLINK_HOME/plugins/s3-fs-presto/
USER root
RUN rm -f /tmp/obsrv-distribution.tar.gz
USER flink
