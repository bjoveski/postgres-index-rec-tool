package database

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/18/13
 * Time: 2:27 PM
 * To change this template use File | Settings | File Templates.
 */

case class Column(name: String, tableName: String) {

  def getTable = {
    Catalog.tables(tableName)
  }
}
