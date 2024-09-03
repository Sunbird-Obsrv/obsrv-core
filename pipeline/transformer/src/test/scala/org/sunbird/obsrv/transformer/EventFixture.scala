package org.sunbird.obsrv.transformer

object EventFixture {

  val SUCCESS_TRANSFORM = """{"dataset":"d1","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val FAILED_TRANSFORM = """{"dataset":"d1","event":{"id":"1235","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"D123","locationId":"KUN1","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val PARTIAL_TRANSFORM = """{"dataset":"d2","event":{"id":"1235","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"D123","locationId":"KUN1","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val SKIPPED_TRANSFORM = """{"dataset":"d3","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val FAILED_TRANSFORM_2 = """{"dataset":"d4","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""

}