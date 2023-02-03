package org.sunbird.obsrv.preprocessor

import com.google.gson.Gson
import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.{Dataset, RouterConfig}
import org.sunbird.obsrv.preprocessor.fixture.EventFixtures
import org.sunbird.obsrv.preprocessor.task.PipelinePreprocessorConfig
import org.sunbird.obsrv.preprocessor.util.SchemaValidator

class TestSchemaValidator extends FlatSpec with Matchers {

  "SchemaValidator" should "return a success report for a valid event" in {
    val config = ConfigFactory.load("test.conf");
    val pipelineProcessorConfig = new PipelinePreprocessorConfig(config)
    val schemaValidator = new SchemaValidator(pipelineProcessorConfig)

    val dataset = Dataset("obs2.0", None, None, None, Option(EventFixtures.schema), None, RouterConfig(""))
    schemaValidator.schemaFileExists(dataset)
    val gson = new Gson()

    val event = JSONUtil.deserialize[Map[String, AnyRef]](EventFixtures.validEvent)
    val report = schemaValidator.validate("obs2.0", event)
    assert(report.isSuccess)
  }

  it should "return a failed validation report for a invalid event" in {
    val config = ConfigFactory.load("test.conf");
    val pipelineProcessorConfig = new PipelinePreprocessorConfig(config)
    val schemaValidator = new SchemaValidator(pipelineProcessorConfig)

    val dataset = Dataset("obs2.0", None, None, None, Option(EventFixtures.schema), None, RouterConfig(""))
    schemaValidator.schemaFileExists(dataset)

    val event = JSONUtil.deserialize[Map[String, AnyRef]](EventFixtures.invalidEvent)
    val report = schemaValidator.validate("obs2.0", event)
    assert(!report.isSuccess)
    assert(report.toString.contains("error: object has missing required properties ([\"obsCode\"])"))

  }

}
