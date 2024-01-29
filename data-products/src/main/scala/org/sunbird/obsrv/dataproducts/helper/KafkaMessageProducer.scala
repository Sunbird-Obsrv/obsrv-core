package org.sunbird.obsrv.dataproducts.helper

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

import java.util.Properties

class KafkaMessageProducer(config: Config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[KafkaMessageProducer])
  private val kafkaProperties = new Properties();
  private val defaultTopicName = config.getString("metrics.topicName")
  private val defaultKey = null

  kafkaProperties.put("bootstrap.servers", config.getString("kafka.bootstrap.servers"))
  kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](kafkaProperties)

  def sendMessage(topic: String = defaultTopicName, key: String = defaultKey, message: String): Unit = {
    try {
      val record = new ProducerRecord[String, String](topic, key, message)
      producer.send(record)
    }
      // $COVERAGE-OFF$
    catch {
      case e: Exception =>
        logger.error("Exception occured while sending message to kafka", e.getMessage)
        e.printStackTrace()
    }
    // $COVERAGE-ON$
  }
}
