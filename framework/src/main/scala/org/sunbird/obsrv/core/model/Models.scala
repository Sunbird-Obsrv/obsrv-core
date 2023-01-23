package org.sunbird.obsrv.core.model

object Models {

  case class PData(val id: String, val `type`: String, val pid: String)
  
  case class SystemEvent(val pdata: PData, data: Map[String, AnyRef] )

}
