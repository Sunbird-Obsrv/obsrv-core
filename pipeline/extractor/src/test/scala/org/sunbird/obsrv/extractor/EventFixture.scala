package org.sunbird.obsrv.extractor

object EventFixture {

  val MISSING_DEDUP_KEY = """{"dataset":"d1","id1":"event1","events":[{"id":"1","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}]}"""
  val INVALID_JSON = """{"dataset":"d1","event":{"id":"2","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}"""

  val VALID_EVENT = """{"dataset":"d1","id":"event4","event":{"id":"3","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val VALID_BATCH = """{"dataset":"d1","id":"event5","events":[{"id":"4","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}]}"""

  val LARGE_JSON_BATCH = """{"dataset":"d1","id":"event2","events":[{"id":"5","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19},"randomKey":"eRJcFJvUoQnlC9ZNa2b2NT84aAv4Trr9m6GFwxaL6Qn1srmWBl7ldsKnBvs6ah2l0KN6M3Vp4eoGLBiIMYsi3gHWklc8sbt6"}]}"""
  val LARGE_JSON_EVENT = """{"dataset":"d1","id":"event3","event":{"id":"6","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19},"randomKey":"eRJcFJvUoQnlC9ZNa2b2NT84aAv4Trr9m6GFwxaL6Qn1srmWBl7ldsKnBvs6ah2l0KN6M3Vp4eoGLBiIMYsi3gHWklc8sbt6"}}"""


}
