package database

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/18/13
 * Time: 2:28 PM
 * To change this template use File | Settings | File Templates.
 */
case class Table(name: String,
            columns: mutable.HashMap[String, Column],
            indices: mutable.HashMap[String, Index]) {

  def insertColumns(cols: List[Column]) {
    cols.foreach(col => columns.put(col.name, col))
  }

  def insertIndices(inds: List[Index]) {
    inds.foreach(ind => indices.put(ind.name, ind))
  }

  override def toString = {
    val indexString = indices.keys.mkString(", ")
    val colString = columns.keys.mkString(", ")
    s"table=[$name],\n columns=[$colString],\n indices=[$indexString]"
  }
}


object Table {
  def apply(name: String) = {
    new Table(name,
      new mutable.HashMap[String, Column](),
      new mutable.HashMap[String, Index]())
  }

}
