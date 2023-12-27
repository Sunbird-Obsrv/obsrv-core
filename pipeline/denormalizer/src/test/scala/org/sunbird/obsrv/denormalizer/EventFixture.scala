package org.sunbird.obsrv.denormalizer

object EventFixture {

  val SUCCESS_DENORM = """{"dataset":"d1","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val SKIP_DENORM = """{"dataset":"d2","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""

  val DENORM_MISSING_KEYS = """{"dataset":"d1","event":{"id":"2345","date":"2023-03-01","dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val DENORM_MISSING_DATA_AND_INVALIDKEY = """{"dataset":"d1","event":{"id":"4567","vehicleCode":["HYUN-CRE-D7"],"date":"2023-03-01","dealer":{"dealerCode":"D124","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val INVALID_DATASET_ID = """{"dataset":"dxyz","event":{"id":"4567","vehicleCode":["HYUN-CRE-D7"],"date":"2023-03-01","dealer":{"dealerCode":"D124","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""
  val MISSING_EVENT_KEY = """{"dataset":"d3","event1":{"id":"4567","vehicleCode":["HYUN-CRE-D7"],"date":"2023-03-01","dealer":{"dealerCode":"D124","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}"""

  val DENORM_DATA_1 = """{"code":"HYUN-CRE-D6","manufacturer":"Hyundai","model":"Creta","variant":"SX(O)","modelYear":"2023","price":"2200000","currencyCode":"INR","currency":"Indian Rupee","transmission":"automatic","fuel":"Diesel"}"""
  val DENORM_DATA_2 = """{"code":"D123","name":"KUN United","licenseNumber":"1234124","authorized":"yes"}"""
}