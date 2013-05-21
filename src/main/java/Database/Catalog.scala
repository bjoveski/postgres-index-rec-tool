package database

import scala.collection.immutable.HashMap
import util.Driver._
import java.sql.ResultSet
import util.{BackendDriver, Driver}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import requests.CreateIndexRequest

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/18/13
 * Time: 2:27 PM
 * To change this template use File | Settings | File Templates.
 */
object Catalog {
  val tables = new mutable.HashMap[String, Table]()
  val columns = new mutable.HashMap[String, Column]()

  def init() {
    val tableNames = BackendDriver.getTableNames

    // insert tables
    tableNames.foreach(name => {
      tables.put(name, Table(name))
    })

    // insert columns
    tables.keys.foreach(tableName => {
      val cols = BackendDriver.getColumnsForTableName(tableName)
      tables(tableName).insertColumns(cols)

      cols.foreach(col => columns.put(col.name, col))
    })



    // insert indices & add them to table
    val inds = BackendDriver.getAllIndices()
    inds.foreach(index => {
      tables(index.table.name).indices.put(index.name, index)
    })

  }

  def getAllColumns = {
    val out = new ListBuffer[Column]()

    tables.values.foreach(table => {
      table.columns.values.foreach(out.append(_))
    })
    out.toList
  }

  def getAllIndices = {
    val out = new ListBuffer[DbIndex]()

    tables.values.foreach(table => {
      table.indices.values.foreach(out.append(_))
    })

    out.toSet
  }

  def dropHypotheticalIndex(index: DbIndex) {
    if (!index.isHypothetical) {
      throw new IllegalArgumentException(s"index $index couldn't be dropped! it's not hypothetical")
    }

    BackendDriver.dropHypotheticalIndex(index)

    val table = index.table
    table.indices.remove(index.name)
  }

  def addHypotheticalIndex(req: CreateIndexRequest) {
    if (!req.isHypothetical) {
      throw new IllegalArgumentException(s"index $req couldn't be added! it's not hypothetical")
    }
    val index = DbIndex(req)
    req.table.indices.put(index.name, index)
    BackendDriver.addHypotheticalIndex(index)
  }

  def runHypotheticalAnalyze(queryString: String) = {
    BackendDriver.getHypotheticalAnalyze(queryString)
  }
}