package database

import util.BackendDriver
import requests.CreateIndexRequest


abstract class Index(val name: String,val columns: List[Column],val table: Table) {
  override def hashCode() = {
    name.hashCode()
  }

  override def equals(obj: Any) = {
    if (!obj.isInstanceOf[Index]) {
      false
    } else {
      obj.asInstanceOf[Index].name == this.name
    }
  }
}

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/18/13
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
case class DbIndex(override val name: String,
           indexDef: String,
           override val columns: List[Column],
           override val table: Table,
           isHypothetical: Boolean) extends Index(name, columns, table) {

  override def toString = {
    val colString =  columns.map(col => col.name)
    s"DbIndex=[$name], table=[${table.name}], columns=[$colString}], " +
    s"isHypothetical=$isHypothetical, \n $indexDef"
  }

}

case class ConfIndex(override val name: String,
                     indexDef: String,
                     override val columns: List[Column],
                     override val table: Table,
                     isHypothetical: Boolean) extends Index(name, columns, table) {

  override def toString = {
    val colString =  columns.map(col => col.name)
    s"ConfIndex=[$name], table=[${table.name}], columns=[$colString}], " +
      s"isHypothetical=$isHypothetical, \n $indexDef"
  }

  def toReq() {
    CreateIndexRequest(table, columns, isHypothetical)
  }
}

object DbIndex {
  val HYPOTHETICAL = "hypothetical"

  /**
   * TO USE ONLY FOR CREATING THE INDEX STRUCTURE AT INIT!!!!
   * DON'T CALL TO CREATE HYPOTHETICAL INDICES
   */
  def getIndexFromDb(name: String,
            indexDef: String,
            tableName: String) = {
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

    new DbIndex(name, indexDef, colmns,
      t, isHypothetical)
  }

  def apply(req: CreateIndexRequest) = {
    new DbIndex(req.generateName(), req.generateSqlString(), req.columns, req.table, req.isHypothetical)
  }
}