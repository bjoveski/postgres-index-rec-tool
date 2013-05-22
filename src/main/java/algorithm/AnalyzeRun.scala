package algorithm

import scala.collection.mutable
import util.{Parser, BackendDriver}
import database.{Catalog, Column}

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/20/13
 * Time: 12:19 AM
 * To change this template use File | Settings | File Templates.
 */
case class AnalyzeRun(query: Query,
                      conf: Configuration,
                      cost: Double,
                      columns: List[Column]) {



  //TODO: do we neeed to add the xml of the thing itself?
}

object AnalyzeRun {
  def run(query: Query, conf: Configuration) {


  }

  def apply(query: Query, conf: Configuration, xmlResult: xml.Elem) = {
    val columns = Parser.getUsedIndexNames(xmlResult).distinct.map(indexName => Catalog.indices(indexName).columns).flatten.toList
    new AnalyzeRun(query, conf, Parser.getTotalCost(xmlResult), columns)
  }
}
