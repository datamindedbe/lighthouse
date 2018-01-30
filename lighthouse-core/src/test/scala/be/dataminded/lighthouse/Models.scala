package be.dataminded.lighthouse

object Models {
  case class RawPerson(name: String, age: Int)
  case class BasePerson(firstName: String, lastName: String, age: Int)
  case class RawCustomer(ID: String, firstName: String, lastName: String, yearOfBirth: String)
  case class RawOrders(id: String, customerId: String)
}
