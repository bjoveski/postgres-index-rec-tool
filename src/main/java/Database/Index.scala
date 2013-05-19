package database

import util.BackendDriver

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/18/13
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */

case class Index(name: String,
           indexDef: SqlStatement,
           columns: List[Column],
           table: Table,
           isHypothetical: Boolean,
           isMaterialized: Boolean) {

  override def toString = {
    val colString =  columns.map(col => col.name)
    s"index=[$name], table=[${table.name}], columns=[${colString}}], " +
    s"isHypothetical=$isHypothetical, isMaterialized=$isMaterialized, \n $indexDef"
  }
}

object Index {
  val HYPOTHETICAL = "hypothetical"


  def apply(name: String,
            indexDef: String,
            tableName: String,
            isMaterialized: Boolean) = {
    if (Catalog.tables.isEmpty) {
      throw new RuntimeException("must init tables first")
    }

    val t = Catalog.tables(tableName)
    val isHypothetical = indexDef.contains(HYPOTHETICAL)

    // get columns
    val colNames = BackendDriver.getIndexColumnNames(name)
    val colmns = t.columns.values.toList.filter(col => colNames.contains(col.name))
    if (colmns.length != colNames.length) {
      throw new RuntimeException("bug!")
    }

    new Index(name, SqlStatement(indexDef), colmns,
      t, isHypothetical, isMaterialized)
  }
}