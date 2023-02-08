package org.sunbird.obsrv.extractor

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.extractor.task.{ExtractorConfig, ExtractorStreamTask}

import java.io.File

object TestExtractorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment()))
    val extractorConfig = new ExtractorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(extractorConfig)
    val task = new ExtractorStreamTask(extractorConfig, kafkaUtil)
    task.process()
  }

}
