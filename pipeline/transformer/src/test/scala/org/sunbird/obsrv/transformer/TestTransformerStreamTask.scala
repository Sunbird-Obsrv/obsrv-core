package org.sunbird.obsrv.transformer

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.transformer.task.{TransformerConfig, TransformerStreamTask}

import java.io.File

object TestTransformerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment()))
    val telemetryExtractorConfig = new TransformerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(telemetryExtractorConfig)
    val task = new TransformerStreamTask(telemetryExtractorConfig, kafkaUtil)
    task.process()
  }
}
