package org.sunbird.obsrv.transformer.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.transformer.task.TransformerConfig

import scala.collection.mutable

class TransformerFunction(config: TransformerConfig)(implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]])
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) {


  override def getMetricsList(): MetricsList = {
    val metrics = List(config.totalEventCount, config.transformSuccessCount,
      config.transformFailedCount, config.transformSkippedCount)
    MetricsList(DatasetRegistry.getDataSetIds(), metrics)
  }


  /**
   * Method to process the event transformations
   */
  override def processElement(msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    val datasetId = msg("dataset").asInstanceOf[String] // DatasetId cannot be empty at this stage
    metrics.incCounter(datasetId, config.totalEventCount)

    val datasetTransformations = DatasetRegistry.getDatasetTransformations(datasetId)
    if(datasetTransformations.isDefined) {
      // TODO: Perform transformations
      metrics.incCounter(datasetId, config.transformSuccessCount)
      context.output(config.transformerOutputTag, markSuccess(msg))
    } else {
      metrics.incCounter(datasetId, config.transformSkippedCount)
      context.output(config.transformerOutputTag, markSkipped(msg))
    }

  }

  private def markSkipped(event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    addFlags(event, Map("transformer_processed" -> "skipped"))
    event
  }

  private def markSuccess(event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    addFlags(event, Map("transformer_processed" -> "yes"))
    event
  }

}

