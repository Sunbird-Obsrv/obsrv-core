package org.sunbird.obsrv.core.reader

trait ParentType {
  def readChild[T]: Option[T]

  def addChild(value: Any): Unit
}
