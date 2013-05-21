package algorithm

import database._
import util.BackendDriver
import requests.CreateIndexRequest
import scala.collection.mutable
import scala.collection.immutable.HashSet
import util.Parser
/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/18/13
 * Time: 2:23 PM
 * To change this template use File | Settings | File Templates.
 */
object Recommender {

  val conf2result = new mutable.HashMap[Configuration, List[AnalyzeRun]]()
  val query2result = new mutable.HashMap[Query, List[AnalyzeRun]]()

  def installConfiguration(conf: Configuration) {
    val currentIndices: Set[Index] = Catalog.getAllIndices.asInstanceOf[Set[Index]]
    val missingIndices = conf.generateIndices.asInstanceOf[Set[Index]] -- currentIndices
    val extraIndices = currentIndices -- conf.generateIndices

    // drop these guys
    extraIndices.foreach(index => {
      index match {
        case r: DbIndex => {
          if (r.isHypothetical) {
            Catalog.dropHypotheticalIndex(r)
          } else {
            // don't drop these guys
            System.out.println(s"skipped dropping realIndex ")
          }}
        case c: ConfIndex => {
          throw new RuntimeException("bug! shouldn't happen that we have extra config")
        }
      }
    })

    // add these guys
    missingIndices.foreach(index => {
      Catalog.addHypotheticalIndex(CreateIndexRequest(index.table, index.columns, true))
    })

    verifyConfiguration(conf)
  }

  def generateConfigurations(numIndices: Int) = {
    val dbIndices = Catalog.getAllIndices
    val realIndices = dbIndices.filter(index => !index.isHypothetical)

    val colsForRealIndices = realIndices.map(inx => inx.columns).flatten.toSet
    val extraColumns = Catalog.getAllColumns.toSet[Column] -- colsForRealIndices

    // generate indices for extra columns
    val subsets = generateSubsets[Column](extraColumns, numIndices)

    subsets.map(subset => {
      // create single indices configurations
      val reqs = createSingleIndexRequests(subset)
      Configuration(reqs)
    })
  }

  /**
   * takes a set of columns and returns a set of single indices over them
   */
  def createSingleIndexRequests(columns: Set[Column]) = {
    columns.map(col => {
      CreateIndexRequest(col.getTable, col :: Nil, isHypothetical = true)
    })
  }

  def runHypotheticalAnalyze(conf: Configuration, query: Query) {
    installConfiguration(conf)
    val xmlRes = Catalog.runHypotheticalAnalyze(query.sqlQuery)

    val res = AnalyzeRun(query, conf, xmlRes)
    updateState(conf, query, res)
  }


  /**
   * calculates total cost for each conf, and chooses the minimal one
   */
  def getBestConfOverall = {
    val costs = conf2result.values.map(confRuns => {
      val totalCost = confRuns.foldLeft[Double](0)((curCost, run) => curCost + run.cost)
      (confRuns.head.conf, totalCost)
    })

    costs.toList.sortBy(_._2)
  }


  def getBestConf(query: Query) = {
    query2result(query).minBy(run => run.cost)
  }

  private def updateState(conf: Configuration, query: Query, res: AnalyzeRun) {
    val lst1 = conf2result.getOrElse(conf, Nil)
    conf2result.update(conf, res :: lst1)

    val lst2 = query2result.getOrElse(query, Nil)
    query2result.update(query, res :: lst2)
  }

  def verifyConfiguration(conf: Configuration) {
    //TODO implement

  }

  def generateSubsets[T](elements: Set[T], subsetSize: Int) = {
    var subsets:mutable.HashSet[Set[T]] = new mutable.HashSet[Set[T]]

    elements.foreach(elem => {
      subsets.add(Set(elem))
    })

    Range(1, subsetSize).foreach( i => {
      subsets = generateNextSubsets[T](subsets, elements)
    })

    subsets
  }

  private def generateNextSubsets[T](subsets: mutable.HashSet[Set[T]], elements: Set[T]) = {
    val out = new mutable.HashSet[Set[T]]()
    elements.foreach(elem => {
      subsets.foreach(subset => {
        if (!subset.contains(elem)) {
          out.add(subset.union(Set(elem)))
        }
      })
    })

    out
  }
}
