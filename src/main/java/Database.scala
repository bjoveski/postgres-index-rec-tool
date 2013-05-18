import scala.collection.immutable.HashMap

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/18/13
 * Time: 12:54 AM
 * To change this template use File | Settings | File Templates.
 */
class Database {


}

class Catalog {
  val tables: Map[String, Table] = new HashMap()// HashMap
  val indices: Map[String, Index] = new HashMap();

}

case class Index(name: String,
            columns: List[Column],
            table: Table,
            isHypothetical: Boolean)

case class Table(name: String,
                 columns: List[Column],
                 indices: List[Index])

case class Column(name: String, table: Table)

