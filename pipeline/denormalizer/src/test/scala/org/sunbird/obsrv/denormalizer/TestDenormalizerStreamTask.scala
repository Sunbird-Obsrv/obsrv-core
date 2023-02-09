package org.sunbird.obsrv.denormalizer

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.denormalizer.task.{DenormalizerConfig, DenormalizerStreamTask}

import java.io.File

object TestDenormalizerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment()))
    val denormalizerConfig = new DenormalizerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(denormalizerConfig)
    val task = new DenormalizerStreamTask(denormalizerConfig, kafkaUtil)
    task.process()
  }

}
