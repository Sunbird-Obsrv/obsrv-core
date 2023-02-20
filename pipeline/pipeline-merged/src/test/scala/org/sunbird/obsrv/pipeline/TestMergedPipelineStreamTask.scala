package org.sunbird.obsrv.pipeline

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.pipeline.task.{MergedPipelineConfig, MergedPipelineStreamTask}

import java.io.File

object TestMergedPipelineStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment()))
    val mergedPipelineConfig = new MergedPipelineConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(mergedPipelineConfig)
    val task = new MergedPipelineStreamTask(config, mergedPipelineConfig, kafkaUtil)
    task.process()
  }

}
