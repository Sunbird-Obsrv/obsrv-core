package org.sunbird.obsrv.core.util

import scala.collection.mutable

object Util {

  def getMutableMap(immutableMap: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val mutableMap = mutable.Map[String, AnyRef]();
    mutableMap ++= immutableMap;
    mutableMap
  }

}
