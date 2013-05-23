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


  /**
   *
   * THE FUNCTION
   */
  def recommend(numIndicesSeed: Int, numIndicesTotal: Int, queries: List[Query]) = {

    // init the indices by calculating the seed
    val seedConfs = naiveImplementation(numIndicesSeed, queries)

    // greedy part of the algorithm
    var bestConf = seedConfs.head._1
    var bestCost = seedConfs.head._2
    var indicesToBeConsidered = generateIndicesToBeConsidered(bestConf, queries).toList
    var isDone = false

    Range(numIndicesSeed, numIndicesTotal).foreach(iteration => {
      if (!isDone) {
        val out = greedyIteration(bestConf, bestCost, queries, indicesToBeConsidered)
        if (out.isEmpty) {
          isDone = true
        } else {
          bestConf = out.get._1
          bestCost = out.get._2
          indicesToBeConsidered = generateIndicesToBeConsidered(bestConf, queries).toList
        }
      } else {
        // skip
      }
    })

    (bestConf, bestCost)
  }



  def generateIndicesToBeConsidered(conf: Configuration, queries: List[Query]) = {
    val columnsToBeConsidered = generateAllRelevantColumns(queries)


    val columnsInCurrentConf = conf.generateIndices.map(index => index.columns).flatten.toSet
    val extraColumns = columnsToBeConsidered -- columnsInCurrentConf

    createSingleIndexRequests(extraColumns)
  }



  def naiveImplementation(numIndices: Int, queries: List[Query]) = {
    val startTime = System.currentTimeMillis()
    val confs = generateConfigurations(numIndices, queries)
    var chptTime = System.currentTimeMillis()
    var count = 0
    System.out.println(s"generatedConfs. count=${confs.size}\ttotalTime=${chptTime - startTime}, \t lastTime=${chptTime - startTime}")

    confs.foreach(conf => {

      queries.foreach(query => {
        //TODO optimization, can get installConf out of here, to the outer loop
        runHypotheticalAnalyze(conf, query)
      })
      count += 1
      if ((count % 1000) == 0) {
        System.out.println(s"processing confs.$count done")
      }
    })
    System.out.println(s"runAnalyze. \ttotalTime=${System.currentTimeMillis() - startTime}, " +
      s"\t lastTime=${System.currentTimeMillis() - chptTime}")
    chptTime = System.currentTimeMillis()

    val out = sortAllConfigurationsByCost
    System.out.println(s"sorted. \ttotalTime=${System.currentTimeMillis() - startTime}, " +
      s"\t lastTime=${System.currentTimeMillis() - chptTime}")

    out

  }



  /**
   *
   * @return None if there was no improvement; or
   *         Some(Configuration, Cost) - the best greedy solution
   */
  def greedyIteration(currentConf: Configuration,
                      currCost: Double,
                      queries: List[Query],
                      indicesToBeConsidered: List[CreateIndexRequest]): Option[(Configuration, Double)] = {
    val confAnalyzeRuns = indicesToBeConsidered.map(indexReq => {
      val conf = Configuration(currentConf, indexReq)

      val analyzeRuns = queries.map(query => {
        runHypotheticalAnalyze(conf, query)
      })

      analyzeRuns
    })

    val sortedConfs = sortConfigurationsByCost(confAnalyzeRuns)

    // current cost is less than the cost with index! stop here
    // because we have no improvement
    if (currCost <= sortedConfs.head._2) {
      None
    } else {
      Some(sortedConfs.head)
    }
  }


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
//            System.out.println(s"skipped dropping realIndex ")
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

  /**
   * generates ALL columns
   */
  private def generateAllColumns() = {
    Catalog.getAllColumns.toSet[Column]
  }

  /**
   * generates columns that appear in JOIN/WHERE clauses
   */
  private def generateAllRelevantColumns(queries: List[Query]) = {
    queries.map(query => {
      query.relevantTableColumnNames.map( tableCol => {
        Catalog.tables(tableCol._1).columns(tableCol._2)
      })
    }).flatten.toSet[Column]
  }


  def generateConfigurations(numIndices: Int, queries: List[Query]) = {
    val dbIndices = Catalog.getAllIndices
    val realIndices = dbIndices.filter(index => !index.isHypothetical)

//    val columnsToBeConsidered = generateAllColumns()
    val columnsToBeConsidered = generateAllRelevantColumns(queries)

    val colsForRealIndices = realIndices.map(inx => inx.columns).flatten.toSet
    val extraColumns = columnsToBeConsidered -- colsForRealIndices

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

  def runHypotheticalAnalyze(conf: Configuration, query: Query) = {
    installConfiguration(conf)
    val xmlRes = Catalog.runHypotheticalAnalyze(query.sqlQuery)

    val res = AnalyzeRun(query, conf, xmlRes)
    updateState(conf, query, res)
    res
  }


  /**
   * calculates total cost for each conf, and chooses the minimal one
   */
  def sortAllConfigurationsByCost = {
    sortConfigurationsByCost(conf2result.values)
  }

  /**
   * @param confAnalyzeRuns the inner list is a list of analyze runs with the same configuration. i.e.
   *                        the conf runs is a a list of analyze runs grouped by their configuration
   * @return (configuration, cost) for the workload
   */
  def sortConfigurationsByCost(confAnalyzeRuns: Iterable[Iterable[AnalyzeRun]]) = {
    val costs = confAnalyzeRuns.map(confRuns => {
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
