package org.sunbird.obsrv.streaming

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

object TestTimestamp {

  def main(args: Array[String]): Unit = {
    val timestampAsString = "2023-10-15T03:56:27.522+05:30"
    val pattern = "yyyy-MM-dd'T'hh:mm:ss.SSSZ"
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    val localDateTime = LocalDateTime.from(formatter.parse(timestampAsString))
    val timestamp = Timestamp.valueOf(localDateTime)
    println("Timestamp: " + timestamp.toString)

  }

}
