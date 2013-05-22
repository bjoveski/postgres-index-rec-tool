package util

import java.sql._
import database._
/**
 * deals with sending requests to the database
 */
object Driver {
  // -------
  // config
  val driverName = "org.postgresql.Driver"
  val url = "jdbc:postgresql:postgres"
  val username = "postgres"
  val password = ""

  var db = DriverManager.getConnection(url, username, password)


  def executeQueryWithStatement[T](query: String, fn: (ResultSet) => T):T = {
    val stmt = db.createStatement()
    val res = stmt.executeQuery(query)
    val out = fn(res)
    stmt.close()
    out
  }

  def executeQueryWithStatement(query: String) {
    val stmt = db.createStatement()
    val success = stmt.execute(query)
    stmt.close()
//    if (!success) {
//      System.out.println(s"query $query returned false!")
//    }
  }

  def closeConnection() {
    db.close()
  }
}
