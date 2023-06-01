package org.sunbird.obsrv.preprocessor.fixture

object EventFixtures {

  val VALID_SCHEMA = """{"$schema":"https://json-schema.org/draft/2020-12/schema","id":"https://sunbird.obsrv.com/test.json","title":"Test Schema","description":"Test Schema","type":"object","properties":{"id":{"type":"string"},"vehicleCode":{"type":"string"},"date":{"type":"string"},"dealer":{"type":"object","properties":{"dealerCode":{"type":"string"},"locationId":{"type":"string"},"email":{"type":"string"},"phone":{"type":"string"}},"required":["dealerCode","locationId"]},"metrics":{"type":"object","properties":{"bookingsTaken":{"type":"number"},"deliveriesPromised":{"type":"number"},"deliveriesDone":{"type":"number"}}}},"required":["id","vehicleCode","date","dealer","metrics"]}"""
  val INVALID_SCHEMA = """{"$schema":"https://json-schema.org/draft/2020-12/schema","id":"https://sunbird.obsrv.com/test.json","title":"Test Schema","description":"Test Schema","type":"object","properties":{"id":{"type":"string"},"vehicleCode":{"type":"string"},"date":{"type":"string"},"dealer":{"type":"object","properties":{"dealerCode":{"type":"string"},"locationId":{"type":"string"},"email":{"type":"string"},"phone":{"type":"string"}},"required":["dealerCode","locationId"]},"metrics":{"type":"object","properties":{"bookingsTaken":{"type":"number"},"deliveriesPromised":{"type":"number"},"deliveriesDone":{"type":"number"}}}},"required":["id","vehicleCode","date","dealer","metrics"}"""


  val VALID_SCHEMA_EVENT = """{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}"""
  val INVALID_SCHEMA_EVENT = """{"id":"1234","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}"""

  val VALID_EVENT = """{"dataset":"d1","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val INVALID_EVENT = """{"dataset":"d1","event":{"id":"1234","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val DUPLICATE_EVENT = """{"dataset":"d1","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val MISSING_DATASET_EVENT = """{"event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val INVALID_DATASET_EVENT = """{"dataset":"dX","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val INVALID_EVENT_KEY = """{"dataset":"d2","event1":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val VALID_EVENT_DEDUP_CONFIG_NONE = """{"dataset":"d2","event":{"id":"1235","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""



}
