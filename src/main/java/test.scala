import net.noerd.prequel.DatabaseConfig

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/17/13
 * Time: 8:53 PM
 * To change this template use File | Settings | File Templates.
 */


// import com.typesafe.slick
//import net.noerd.prequel.DatabaseConfig
//import net.noerd.prequel.SQLFormatterImplicits._
//import net.noerd.prequel.ResultSetRowImplicits._

import java.sql._

class test {

  val res = Driver.getHypotheticalAnalyze("SELECT * FROM oorder WHERE o_w_id=1 AND o_d_id=5")





//  val db = DatabaseConfig(
//    driver = "org.postgresq.Driver",
//    jdbcURL = "jdbc:postgresql://localhost"
//  )
//  Class.forName("org.postgresql.Driver")
//
//  val url = "jdbc:postgresql:postgres"
//  val username = "postgres"
//  val password = ""
//  val db = DriverManager.getConnection(url, username, password);
//
//  val stmt = db.createStatement()
////  val res = stmt.executeQuery("explain (format xml) SELECT * FROM oorder WHERE o_w_id=1 AND o_d_id=5;")
//  val res = stmt.executeQuery("explain hypothetical SELECT * FROM oorder WHERE o_w_id=1 AND o_d_id=5;")
//
//  while (res.next()) {
//    printf(res.getSQLXML(1).getString)
//  }
//  // val bla =  new slick.driver.
//
//  def bla(i:Int)  {
//    printf("hello %d", i)
//  }

}
