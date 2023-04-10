package org.sunbird.obsrv.core.util

import org.apache.flink.api.java.functions.KeySelector

import scala.collection.mutable

class DatasetKeySelector() extends KeySelector[mutable.Map[String, AnyRef], String] {

  override def getKey(in: mutable.Map[String, AnyRef]): String = {
    in("dataset").asInstanceOf[String]
  }
}