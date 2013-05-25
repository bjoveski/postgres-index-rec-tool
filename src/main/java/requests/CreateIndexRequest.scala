package requests

import database._

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/20/13
 * Time: 10:05 AM
 * To change this template use File | Settings | File Templates.
 */
case class CreateIndexRequest(table: Table,
                              columns: List[Column],
                              isHypothetical: Boolean) {

  def generateName() = {
    val colStr = columns.map(col => col.name).sorted.mkString("_")

    if (isHypothetical) {
      s"idx_${DbIndex.HYPOTHETICAL}_$colStr"
    } else {
      s"idx_$colStr"
    }
  }


  override def toString = {
    val colString =  columns.map(col => col.name)
    s"CreateIndexReq=[$generateName], table=[${table.name}] ], "
  }

  def generateSqlString() = {
    val colStr = columns.map(_.name).mkString(", ")
    if (isHypothetical) {
      Procedures.createHypotheticalIndex.
        getInstantiatedQuery(generateName(), table.name, colStr)
    } else {
      Procedures.createRealIndex.
        getInstantiatedQuery(generateName(), table.name, colStr)
    }
  }

  def generateIndex() = {
    ConfIndex(generateName(), generateSqlString(), columns, table, true)
  }
}
