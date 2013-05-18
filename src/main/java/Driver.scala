import java.sql._

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/18/13
 * Time: 12:13 AM
 * To change this template use File | Settings | File Templates.
 */
object Driver {
  // -------
  // config
  val driverName = "org.postgresql.Driver"
  val url = "jdbc:postgresql:postgres"
  val username = "postgres"
  val password = ""

  val db = DriverManager.getConnection(url, username, password)


  def getHypotheticalAnalyze(explainQuery: String) = {
    val query = s"explain hypothetical $explainQuery"
    val xmlBuilder = new StringBuilder()

    executeQueryWithStatement(query,
      (res: ResultSet) => {
        while (res.next()) {
          xmlBuilder.append(res.getSQLXML(1).getString)
        }
      }
    )
//    xmlBuilder.toString()
    scala.xml.XML.loadString(xmlBuilder.toString())
  }

  def addHypotheticalIndex(index: Index) {
    addHypotheticalIndex(index.name, index.table, index.columns)
  }

  def addHypotheticalIndex(name: String, t: Table, cols: List[Column]) {
    val colString = cols.map(_.name).mkString("(", ", ", ")")

    val query = s"CREATE HYPOTHETICAL INDEX $name ON ${t.name} $colString)"
    executeQueryWithStatement(query)
  }

  def dropHypotheticalIndex(index: Index) {
    if (!index.isHypothetical) {
      throw new IllegalArgumentException(s"$index is not hypothetical! bug")
    }
    val query = s"DROP HYPOTHETICAL INDEX ${index.name}"
    executeQueryWithStatement(query)
  }




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
    if (!success) {
      throw new RuntimeException(s"query $query failed!")
    }
  }

  def closeConnection() {
    db.close()
  }
}
