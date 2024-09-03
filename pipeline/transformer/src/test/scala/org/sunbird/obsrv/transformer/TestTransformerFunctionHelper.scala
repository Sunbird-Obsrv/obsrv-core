package org.sunbird.obsrv.transformer

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.scalatest.Matchers
import org.sunbird.obsrv.core.model.{ErrorConstants, StatusCode}
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.{Condition, DatasetTransformation, TransformationFunction}
import org.sunbird.obsrv.model.TransformMode
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry
import org.sunbird.obsrv.transformer.functions.TransformerFunctionHelper
import org.sunbird.obsrv.transformer.util.{CipherUtil, ConditionEvaluator}
import org.sunbird.obsrv.transformer.types._

class TestTransformerFunctionHelper extends BaseSpecWithDatasetRegistry with Matchers {

  implicit val jsonFormats: DefaultFormats.type = DefaultFormats

  implicit class JsonHelper(json: JValue) {
    def customExtract[T](path: String)(implicit mf: Manifest[T]): T = {
      path.split('.').foldLeft(json)({ case (acc: JValue, node: String) => acc \ node }).extract[T]
    }
  }

  val jsonStr = """{"obsCode":"M_BATTERY_CHARGE","accountEmail":"firstname.lastname@gmail.com","accountPhone":"123456","codeComponents":[{"componentCode":"CC_METADATA_DEVICE_FIRMWARE_VER","componentType":"METADATA_DEVICE","selector":"FIRMWARE_VERSION","value":"2.3"}],"phenTime":"2022-06-17T07:12:02Z","valueUoM":"prcnt","value":"100","id":"df4c7aa4-65df-4463-b92a-7a29835f9c4d","parentCollectionRef":"41e9b7a4-5b6f-11ed-8fd5-a6a5696c2aaa","created":"2022-11-03T12:01:32Z","modified":1667476892000,"integrationAccountRef":"zzz11120-f0c8-4064-8d00-a73e58939ce0_mtgc203d-2478-4679-a0ef-d736a7a406fd","assetRef":"9422f7ac-c6e9-5c72-b605-5a7655863866","assetRef2":"","assetRef4":123124,"testBool":false,"contextItems":[{"code":"SYN_SYSTEM","value":"VALENCO"}],"status":"ACTIVE","xMin":3.356701,"xMax":3.356701,"yMin":51.01653,"yMax":51.01653,"spatialExtent":"{\"type\": \"Point\", \"coordinates\": [3.356701, 51.016530]}","phenEndTime":"2022-06-17T07:12:02Z","value_double_type":100.0}"""
  val mapper = new ObjectMapper()
  val jsonNode: JsonNode = mapper.readTree(jsonStr)

  "TransformerFunctionHelper" should "mask the events for the given transformation config" in {

    val json = parse(jsonStr)
    val dtList = Option(List(
      DatasetTransformation("tf1", "obs2.0", "spatialExtent", TransformationFunction("mask", None, "spatialExtent")),
      DatasetTransformation("tf1", "obs2.0", "assetRef", TransformationFunction("mask", None, "assetRef")),
      DatasetTransformation("tf1", "obs2.0", "accountEmail", TransformationFunction("mask", Some(Condition("jsonata", "obsCode='M_BATTERY_CHARGE' and accountEmail='firstname.lastname@gmail.com' and $number(value)>=100")), "accountEmail")),
      DatasetTransformation("tf1", "obs2.0", "accountPhone2", TransformationFunction("mask", None, "accountPhone")),
      DatasetTransformation("tf1", "obs2.0", "codeComponentsList", TransformationFunction("jsonata", Some(Condition("jsonata", "obsCode='M_BATTERY_CHARGE' and accountEmail='firstname.lastname@gmail.com' and $number(value)>=100")), "$keys(codeComponents)")),
      DatasetTransformation("tf1", "obs2.0", "valueAsInt", TransformationFunction("jsonata", None, "$number(value)")),
      DatasetTransformation("tf1", "obs2.0", "firmwareComponent", TransformationFunction("jsonata", None, "codeComponents[0]")),
      DatasetTransformation("tf1", "obs2.0", "optionalValue", TransformationFunction("jsonata", None, "$number(optionValue)"))
    ))

    val result = TransformerFunctionHelper.processTransformations(json, dtList)
    result.status should be(StatusCode.success)
    result.fieldStatus.size should be(8)
    assert(result.resultJson.customExtract[String]("spatialExtent").equals("{type: ***********************************1.016530]}"))
    assert(result.resultJson.customExtract[String]("assetRef").equals("9422f7***********************5863866"))
    assert(result.resultJson.customExtract[String]("accountEmail").equals("fi***************e@gmail.com"))
    assert(result.resultJson.customExtract[String]("accountPhone2").equals("1***56"))
    assert(JSONUtil.getKey("optionalValue", JSONUtil.serialize(result.resultJson)).isMissingNode.equals(true))

    val dtList2 = Option(List(
      DatasetTransformation("tf1", "obs2.0", "accountPhone", TransformationFunction("mask", Some(Condition("jsonata", "obsCode='M_BATTERY_CHARGE1'")), "accountPhone"), Some(TransformMode.Lenient)),
      DatasetTransformation("tf4", "obs2.0", "asset.assetRef2", TransformationFunction("mask", None, "assetRef2"), Some(TransformMode.Lenient)),
      DatasetTransformation("tf5", "obs2.0", "asset.assetRef3", TransformationFunction("mask", None, "assetRef3"), Some(TransformMode.Lenient)),
      DatasetTransformation("tf6", "obs2.0", "asset.assetRef4", TransformationFunction("mask", None, "assetRef4"), Some(TransformMode.Lenient)),
      DatasetTransformation("tf7", "obs2.0", "asset.assetRef5", TransformationFunction("custom", None, "join(d2.assetRef4)"), Some(TransformMode.Lenient))
    ))
    val result2 = TransformerFunctionHelper.processTransformations(json, dtList2)
    result2.status should be(StatusCode.partial)
    result2.fieldStatus.size should be(4)
    result2.resultJson.customExtract[String]("asset.assetRef2") should be("")
    result2.resultJson.customExtract[String]("asset.assetRef3") should be(null)
    result2.resultJson.customExtract[String]("asset.assetRef4") should be("1***24")
    result.resultJson.customExtract[String]("accountPhone") should be ("123456")

    val result3 = TransformerFunctionHelper.processTransformations(json, None)
    result3.status should be (StatusCode.skipped)
    result3.fieldStatus.size should be(0)
  }

  it should "validate the jsonata expressions" in {

    val json = parse(jsonStr)
    val dtList = Option(List(
      DatasetTransformation("tf1", "obs2.0", "codeComponentsList", TransformationFunction("jsonata", Some(Condition("jsonata", "obsCode='M_BATTERY_CHARGE' and accountEmail='firstname.lastname@gmail.com' and $number(value)>=100")), "$keys(codeComponents).length"), Some(TransformMode.Lenient)),
      DatasetTransformation("tf1", "obs2.0", "valueAsInt", TransformationFunction("jsonata", None, "$number(value)")),
      DatasetTransformation("tf1", "obs2.0", "firmwareComponent", TransformationFunction("jsonata", None, "codeComponents[0]"))
    ))
    val result = TransformerFunctionHelper.processTransformations(json, dtList)
    result.status should be(StatusCode.partial)
    result.fieldStatus.size should be(3)
    assert(result.resultJson.customExtract[String]("firmwareComponent.componentCode").equals("CC_METADATA_DEVICE_FIRMWARE_VER"))
    assert(result.resultJson.customExtract[Int]("valueAsInt").equals(100))
  }

  it should "handle the jsonata parse and eval exceptions including transformation modes" in {

    val json = parse(jsonStr)
    val dtList = Option(List(
      DatasetTransformation("tf1", "obs2.0", "codeComponentsList", TransformationFunction("jsonata", Some(Condition("jsonata", "obsCode='M_BATTERY_CHARGE' and accountEmail='firstname.lastname@gmail.com' and $number(value)>=100")), "$keys(codeComponent).length")),
      DatasetTransformation("tf1", "obs2.0", "valueAsInt", TransformationFunction("jsonata", None, "number(value)")),
      DatasetTransformation("tf1", "obs2.0", "valueAsInt2", TransformationFunction("jsonata", None, null), Some(TransformMode.Lenient)),
      DatasetTransformation("tf1", "obs2.0", "firmwareComponent", TransformationFunction("jsonata", None, "codeComponents[0]"))
    ))
    val result = TransformerFunctionHelper.processTransformations(json, dtList)
    result.status should be(StatusCode.failed)
    result.fieldStatus.size should be(4)
    result.fieldStatus.count(f => f.error.isDefined && f.error.get.equals(ErrorConstants.ERR_UNKNOWN_TRANSFORM_EXCEPTION)) should be(1)
    result.fieldStatus.count(f => f.error.isDefined && f.error.get.equals(ErrorConstants.ERR_EVAL_EXPR_FUNCTION)) should be(1)
    result.fieldStatus.count(f => f.error.isDefined && f.error.get.equals(ErrorConstants.INVALID_EXPR_FUNCTION)) should be(1)
    result.fieldStatus.foreach { status: TransformFieldStatus => {
      status.fieldKey match {
        case "codeComponentsList" =>
          status.expr should be("$keys(codeComponent).length")
          status.success should be(false)
          status.mode should be(TransformMode.Strict)
          status.error.get should be(ErrorConstants.ERR_EVAL_EXPR_FUNCTION)
        case "valueAsInt" =>
          status.expr should be("number(value)")
          status.success should be(false)
          status.mode should be(TransformMode.Strict)
          status.error.get should be(ErrorConstants.INVALID_EXPR_FUNCTION)
        case "firmwareComponent" =>
          status.expr should be("codeComponents[0]")
          status.success should be(true)
          status.mode should be(TransformMode.Strict)
          status.error should be(None)
        case "valueAsInt2" =>
          status.expr should be(null)
          status.success should be(false)
          status.mode should be(TransformMode.Lenient)
          status.error.get should be(ErrorConstants.ERR_UNKNOWN_TRANSFORM_EXCEPTION)
      }
    }
    }
  }

  it should "encrypt the fields in the event" in {
    val json = parse(jsonStr)
    val dtList = Option(List(
      DatasetTransformation("tf1", "obs2.0", "accountEmail", TransformationFunction("encrypt", None, "accountEmail")),
      DatasetTransformation("tf2", "obs2.0", "accountPhone", TransformationFunction("encrypt", None, "accountPhone")),
      DatasetTransformation("tf3", "obs2.0", "assetRef", TransformationFunction("encrypt", None, "assetRef")),
      DatasetTransformation("tf4", "obs2.0", "asset.assetRef2", TransformationFunction("encrypt", None, "assetRef2")),
      DatasetTransformation("tf5", "obs2.0", "asset.assetRef3", TransformationFunction("encrypt", None, "assetRef3")),
      DatasetTransformation("tf6", "obs2.0", "asset.assetRef4", TransformationFunction("encrypt", None, "assetRef4"))
    ))
    val result = TransformerFunctionHelper.processTransformations(json, dtList)
    val jsonData = compact(render(result.resultJson))
    result.status should be(StatusCode.failed)
    result.fieldStatus.size should be(6)
    assert(result.resultJson.customExtract[String]("accountEmail").equals("jyx7+dUfzHgODno2jcp67/rfCvOecaLLWICRnSCNvzY="))
    assert(result.resultJson.customExtract[String]("accountPhone").equals("qqyhkaWkPR3t1k0swyQ7Ow=="))
    assert(result.resultJson.customExtract[String]("assetRef").equals("e+YNIi1FebmPPI7D8k3/idlQ8XX0AIhuplwcRLbPb3nkS25gt/HyUQkWeuj6KPxf"))
    result.resultJson.customExtract[String]("asset.assetRef2") should be("")
    result.resultJson.customExtract[String]("asset.assetRef4") should be("D2ySyi1WGqJsM4mbIjbtJA==")
    result.resultJson.customExtract[String]("asset.assetRef3") should be(null)

    JSONUtil.getKey("asset.assetRef3", jsonData).isEmpty should be(true)

    assert(CipherUtil.decrypt(result.resultJson.customExtract[String]("accountEmail")).equals("firstname.lastname@gmail.com"))
    assert(CipherUtil.decrypt(result.resultJson.customExtract[String]("accountPhone")).equals("123456"))
    assert(CipherUtil.decrypt(result.resultJson.customExtract[String]("assetRef")).equals("9422f7ac-c6e9-5c72-b605-5a7655863866"))
  }

  it should "validate all scenarios of condition evaluator" in {
    val status1 = ConditionEvaluator.evalCondition("d1", jsonNode, Some(Condition("custom", "testExpr")), Some(TransformMode.Strict))
    status1.expr should be("")
    status1.success should be(false)
    status1.mode.get should be(TransformMode.Strict)
    status1.error.get should be(ErrorConstants.NO_IMPLEMENTATION_FOUND)

    val status2 = ConditionEvaluator.evalCondition("d1", jsonNode, Some(Condition("jsonata", "number(value)")), Some(TransformMode.Strict))
    status2.expr should be("number(value)")
    status2.success should be(false)
    status2.mode.get should be(TransformMode.Strict)
    status2.error.get should be(ErrorConstants.INVALID_EXPR_FUNCTION)

    val status3 = ConditionEvaluator.evalCondition("d1", jsonNode, Some(Condition("jsonata", "$keys(codeComponent).length")), Some(TransformMode.Strict))
    status3.expr should be("$keys(codeComponent).length")
    status3.success should be(false)
    status3.mode.get should be(TransformMode.Strict)
    status3.error.get should be(ErrorConstants.ERR_EVAL_EXPR_FUNCTION)

    val status4 = ConditionEvaluator.evalCondition("d1", jsonNode, Some(Condition("jsonata", null)), Some(TransformMode.Strict))
    status4.expr should be(null)
    status4.success should be(false)
    status4.mode.get should be(TransformMode.Strict)
    status4.error.get should be(ErrorConstants.ERR_UNKNOWN_TRANSFORM_EXCEPTION)

    val status5 = ConditionEvaluator.evalCondition("d1", null, Some(Condition("jsonata", "$number(value)")), Some(TransformMode.Lenient))
    status5.expr should be("$number(value)")
    status5.success should be(false)
    status5.mode.get should be(TransformMode.Lenient)
    status5.error.get should be(ErrorConstants.ERR_UNKNOWN_TRANSFORM_EXCEPTION)
  }

  it should "cover the unreachable code block in ITransformer" in {
    val testTransformer = new TestTransformer()
    val res1 = testTransformer.getJSON("event.key", null.asInstanceOf[JsonNode])
    compact(render(res1)) should be("""{"event":{"key":null}}""")
    val res2 = testTransformer.getJSON("event.key.x", JSONUtil.getKey("obsCode", jsonStr))
    compact(render(res2)) should be("""{"event":{"key":{"x":"M_BATTERY_CHARGE"}}}""")
    val res3 = testTransformer.getJSON("event.key.y", JSONUtil.getKey("testBool", jsonStr))
    compact(render(res3)) should be("""{"event":{"key":{"y":false}}}""")

    val res4 = testTransformer.transform(parse(jsonStr), jsonNode, List[DatasetTransformation]())
    res4.json should be(JNothing)
    res4.fieldStatus.size should be(0)
  }

}

class TestTransformer extends ITransformer {
  override def transformField(json: JValue, jsonNode: JsonNode, dt: DatasetTransformation): (JValue, TransformFieldStatus) = {
    (JNothing, TransformFieldStatus("", "", success = false, TransformMode.Lenient))
  }

}