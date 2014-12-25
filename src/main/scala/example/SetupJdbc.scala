package example

import scalikejdbc._

object SetupJdbc {
  val host = "localhost"
  val driver = "org.postgresql.Driver"
  val user = "cody"
  val password = ""

  def apply(): Unit = {
    Class.forName(driver)
    ConnectionPool.singleton(host, user, password)
  }
}
