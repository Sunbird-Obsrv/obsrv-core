package org.sunbird.obsrv.core.util

import java.lang.reflect.{ParameterizedType, Type}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator.Feature
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule, ScalaObjectMapper}

import scala.collection.mutable

object JSONUtil {

  @transient private val mapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    .enable(Feature.WRITE_BIGDECIMAL_AS_PLAIN)
    .build() :: ClassTagExtensions

  mapper.setSerializationInclusion(Include.NON_NULL)

  @throws(classOf[Exception])
  def serialize(obj: AnyRef): String = {
    mapper.writeValueAsString(obj)
  }

  def deserialize[T: Manifest](json: String): T = {
    mapper.readValue(json, typeReference[T])
  }

  def deserialize[T: Manifest](json: Array[Byte]): T = {
    mapper.readValue(json, typeReference[T])
  }

  def isJSON(jsonString: String): Boolean = {
    try {
      mapper.readTree(jsonString)
      true
    } catch {
      case _: Exception => false
    }
  }

  def convertValue(map: Map[String, AnyRef]): JsonNode = {
    mapper.convertValue[JsonNode](map, classOf[JsonNode])
  }

  def getKey(key: String, json: String): JsonNode = {
    val path = "/" + key.replaceAll("\\.", "/");
    val root = mapper.readTree(json);
    root.at(path);
  }

  private[this] def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType: Type = typeFromManifest(manifest[T])
  }


  private[this] def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) { m.runtimeClass }
    else new ParameterizedType {
      def getRawType: Class[_] = m.runtimeClass
      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray
      // $COVERAGE-OFF$
      def getOwnerType = null
      // $COVERAGE-ON$
    }
  }

}
