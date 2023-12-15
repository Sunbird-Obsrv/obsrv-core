package org.sunbird.obsrv.fixture

object EventFixture {

  val VALID_BATCH_EVENT_D3_INSERT = """{"dataset":"d3","id":"event1","events":[{"code":"HYUN-CRE-D6","manufacturer":"Hyundai","model":"Creta","variant":"SX(O)","modelYear":"2023","price":"2200000","currencyCode":"INR","currency":"Indian Rupee","transmission":"automatic","fuel":"Diesel"}]}"""
  val VALID_BATCH_EVENT_D3_INSERT_2 = """{"dataset":"d3","id":"event1","events":[{"code":"HYUN-TUC-D6","manufacturer":"Hyundai","model":"Tucson","variant":"Signature","modelYear":"2023","price":"4000000","currencyCode":"INR","currency":"Indian Rupee","transmission":"automatic","fuel":"Diesel"}]}"""
  val VALID_BATCH_EVENT_D3_UPDATE = """{"dataset":"d3","id":"event1","events":[{"code":"HYUN-CRE-D6","safety":"3 Star (Global NCAP)","seatingCapacity":5}]}"""
  val VALID_BATCH_EVENT_D4 = """{"dataset":"d4","event":{"code":"JEEP-CP-D3","manufacturer":"Jeep","model":"Compass","variant":"Model S (O) Diesel 4x4 AT","modelYear":"2023","price":"3800000","currencyCode":"INR","currency":"Indian Rupee","transmission":"automatic","fuel":"Diesel","safety":"5 Star (Euro NCAP)","seatingCapacity":5}}"""
  val MISSING_DATA_KEY_EVENT_D4 = """{"dataset":"d5","event":{"code1":"JEEP-CP-D3","manufacturer":"Jeep","model":"Compass","variant":"Model S (O) Diesel 4x4 AT","modelYear":"2023","price":"3800000","currencyCode":"INR","currency":"Indian Rupee","transmission":"automatic","fuel":"Diesel","safety":"5 Star (Euro NCAP)","seatingCapacity":5}}"""
}
