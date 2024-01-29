package org.sunbird.obsrv.dataproducts.util

import kong.unirest.{HttpResponse, JsonNode, Unirest}

import scala.collection.JavaConverters._
import scala.language.postfixOps

object HttpUtil extends Serializable {

  def post(url: String, requestBody: String, headers: Map[String, String] = Map[String, String]("Content-Type" -> "application/json")): HttpResponse[JsonNode] = {
    Unirest.post(url).headers(headers.asJava).body(requestBody).asJson()
  }

  def delete(url: String): HttpResponse[JsonNode] = {
    Unirest.delete(url).header("Content-Type", "application/json").asJson()
  }
}
