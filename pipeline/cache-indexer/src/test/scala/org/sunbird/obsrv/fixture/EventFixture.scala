package org.sunbird.obsrv.fixture

object EventFixture {

  val VALID_BATCH_EVENT_D3_INSERT = """{"code":"HYUN-CRE-D6","manufacturer":"Hyundai","model":"Creta","variant":"SX(O)","modelYear":"2023","price":"2200000","currencyCode":"INR","currency":"Indian Rupee","transmission":"automatic","fuel":"Diesel","dealer":{"email":"john.doe@example.com","locationId":"KUN12345"}}"""
  val VALID_BATCH_EVENT_D3_INSERT_2 = """{"code":"HYUN-TUC-D6","manufacturer":"Hyundai","model":"Tucson","variant":"Signature","modelYear":"2023","price":"4000000","currencyCode":"INR","currency":"Indian Rupee","transmission":"automatic","fuel":"Diesel","dealer":{"email":"admin.hyun@gmail.com","locationId":"KUN134567"}}"""
  val VALID_BATCH_EVENT_D3_UPDATE = """{"code":"HYUN-CRE-D6","dealer":{"email":"john.doe@example.com","locationId":"KUN12345"},"safety":"3 Star (Global NCAP)","seatingCapacity":5}"""
  val VALID_BATCH_EVENT_D4 = """{"code":"JEEP-CP-D3","manufacturer":"Jeep","model":"Compass","variant":"Model S (O) Diesel 4x4 AT","modelYear":"2023","price":"3800000","currencyCode":"INR","currency":"Indian Rupee","transmission":"automatic","fuel":"Diesel","safety":"5 Star (Euro NCAP)","seatingCapacity":5}"""
  val INVALID_BATCH_EVENT_D4 = """{"code1":"JEEP-CP-D3","manufacturer":"Jeep","model":"Compass","variant":"Model S (O) Diesel 4x4 AT","modelYear":"2023","price":"3800000","currencyCode":"INR","currency":"Indian Rupee","transmission":"automatic","fuel":"Diesel","safety":"5 Star (Euro NCAP)","seatingCapacity":5}"""
}
