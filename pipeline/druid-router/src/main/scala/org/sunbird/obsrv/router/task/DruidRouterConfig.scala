package org.sunbird.obsrv.router.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class DruidRouterConfig(override val config: Config) extends BaseJobConfig(config, "DruidValidatorJob") {

  private val serialVersionUID = 2905979434303791379L
  implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")

  // Router job metrics
  val routerTotalCount = "router-total-count"
  val routerSuccessCount = "router-success-count"


  // Consumers
  val druidRouterConsumer = "druid-router-consumer"

  // Functions
  val druidRouterFunction = "DruidRouterFunction"

  // Producers
  val druidRouterProducer = "druid-router-sink"

}
