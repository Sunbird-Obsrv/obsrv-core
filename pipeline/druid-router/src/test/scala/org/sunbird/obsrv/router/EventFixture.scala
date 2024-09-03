package org.sunbird.obsrv.router

object EventFixture {

  val SUCCESS_EVENT = """{"dataset":"d1","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val FAILED_EVENT = """{"dataset":"d2","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
}
