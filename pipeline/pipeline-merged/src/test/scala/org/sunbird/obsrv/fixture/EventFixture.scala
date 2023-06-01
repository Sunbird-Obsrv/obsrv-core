package org.sunbird.obsrv.fixture

object EventFixture {

  val VALID_BATCH_EVENT_D1 = """{"dataset":"d1","id":"event1","events":[{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}]}"""
  val MISSING_DATASET_BATCH_EVENT = """{"events":[{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}]}"""
  val UNREGISTERED_DATASET_BATCH_EVENT = """{"dataset":"dX","events":[{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}]}"""
  val DUPLICATE_BATCH_EVENT = """{"dataset":"d1","id":"event1","events":[{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}]}"""
  val INVALID_BATCH_EVENT_INCORRECT_EXTRACTION_KEY = """{"dataset":"d1","id":"event2","eve":[{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}]}"""
  val INVALID_BATCH_EVENT_EXTRACTION_KEY_NOT_ARRAY = """{"dataset":"d1","id":"event3","events":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""

  val VALID_BATCH_EVENT_D2 = """{"dataset":"d2","id":"event4","event":{"id":"4567","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val INVALID_BATCH_EVENT_D2 = """{"dataset":"d2","id":"event5","event1":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""


}
