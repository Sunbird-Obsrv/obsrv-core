package org.sunbird.obsrv.core.util

import org.apache.http.client.methods.{HttpGet, HttpRequestBase}
import org.apache.http.impl.client.HttpClients

import scala.io.Source

class RestUtil extends Serializable {

    def get(url: String, headers: Option[Map[String, String]] = None) : String = {
        val httpClient = HttpClients.createDefault()
        val request = new HttpGet(url)
        headers.getOrElse(Map()).foreach {
            case (headerName, headerValue) => request.addHeader(headerName, headerValue)
        }
        try {
            val httpResponse = httpClient.execute(request.asInstanceOf[HttpRequestBase])
            val entity = httpResponse.getEntity
            val inputStream = entity.getContent
            val content = Source.fromInputStream(inputStream, "UTF-8").getLines.mkString
            inputStream.close()
            content

        } finally {
            httpClient.close()
        }

    }


}