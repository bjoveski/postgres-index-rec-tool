package util

import java.sql.ResultSet
import database._
import Driver._
import scala.collection.mutable.ListBuffer
import database.Table
import database.DbIndex
import database.Column

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/19/13
 * Time: 9:45 AM
 * To change this template use File | Settings | File Templates.
 */
object BackendDriver {


  /**
   *
   * @note must be called after we populate tables
   */
  def getAllIndices() = {
    val query = Procedures.selectAllIndices.getInstantiatedQuery()
    val out = new ListBuffer[DbIndex]()

    executeQueryWithStatement(query,
      {(res:ResultSet) => {
        while (res.next()) {
          val indexName = res.getString("indexname")
          val tableName = res.getString("tablename")
          val indexDef = res.getString("indexdef")

          val index = DbIndex.getIndexFromDb(indexName, indexDef, tableName)
          out += index
        }
      }}
    )

    out.toList
  }

  def getIndexColumnNames(indexName: String) = {
    val query = Procedures.selectIndexColumns.getInstantiatedQuery(indexName)

    val out: ListBuffer[String] = new ListBuffer[String]()

    executeQueryWithStatement(query,
    {(res:ResultSet) => {
      while (res.next()) {
        val columnName = res.getString("column_name")
        out += columnName
      }
    }}
    )
    out.toList
  }

  def getColumnsForTableName(tableName: String) = {
    val query = Procedures.selectColumnNames.getInstantiatedQuery(tableName)
    val out: ListBuffer[Column] = new ListBuffer[Column]()

    executeQueryWithStatement(query,
      {(res: ResultSet) => {
        while (res.next()) {
          val colName = res.getString("column_name")
          val col = Column(colName, tableName)
          out += col
        }
      }}
    )

    out.toList
  }

  def getTableNames: List[String] = {
    val query = Procedures.selectAllTables.getInstantiatedQuery()
    val out:ListBuffer[String] = new ListBuffer[String]()

    executeQueryWithStatement(query,
      {(res: ResultSet) => {
        while (res.next()) {
          val tableName = res.getString("relname")
          out += tableName
        }
      }
    })

    out.toList
  }

  def getHypotheticalAnalyze(explainQuery: String) = {
    val query = s"EXPLAIN HYPOTHETICAL $explainQuery"
    val xmlBuilder = new StringBuilder()

    executeQueryWithStatement(query,
      (res: ResultSet) => {
        while (res.next()) {
          xmlBuilder.append(res.getSQLXML(1).getString)
        }
      }
    )
    scala.xml.XML.loadString(xmlBuilder.toString())
  }

  def addHypotheticalIndex(index: DbIndex) {
    val query = index.indexDef
    executeQueryWithStatement(query)
  }


  def dropHypotheticalIndex(index: DbIndex) {
    if (!index.isHypothetical) {
      throw new IllegalArgumentException(s"$index is not hypothetical! bug")
    }
    val query = s"DROP HYPOTHETICAL INDEX ${index.name}"
    executeQueryWithStatement(query)
  }



}
