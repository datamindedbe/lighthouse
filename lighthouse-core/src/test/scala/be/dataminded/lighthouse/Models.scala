package be.dataminded.lighthouse

object Models {
  case class RawCustomer(ID: String, FIRST_NAME: String, LAST_NAME: String, YEAR_OF_BIRTH: String)
  case class RawOrders(ID: String, CUSTOMER_ID: String)
}
