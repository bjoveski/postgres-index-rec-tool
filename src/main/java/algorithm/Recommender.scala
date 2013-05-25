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
  val cols2multiIndices = new mutable.HashSet[Column]()
  val multiIndicesThatAreUsed = new mutable.HashMap[String, CreateIndexRequest]()
  val usedIndices = new mutable.HashMap[String, CreateIndexRequest]()

  /**
   *
   * THE FUNCTION
   */
  def recommend(numIndicesSeed: Int, numIndicesTotal: Int, queries: List[Query]) = {

    // init the indices by calculating the seed
    // i.e. running all single-index stuff that are used
    val seedConfs = naiveImplementation(numIndicesSeed, queries)

    System.out.println(s"numConfsFromSeed=${seedConfs.size}, cols2mutli=${cols2multiIndices.size}")

    // find multi col indices
    val multiColIndices = generate2ColIndices(queries)
    System.out.println(s"\n\n\nmultiColIndices=${multiColIndices.size}")


    // generate good base Configurations with multi indices
    seedConfs.take(10).map(topConf => {
      val conf = topConf._1
      val cost = topConf._2

      multiColIndices.foreach(multiColIndex => {
        val newConf = Configuration(conf, multiColIndex)
        installConfiguration(newConf)
        queries.foreach(query => {
          val run = runHypotheticalAnalyzeWithooutInstalling(conf, query)
          run.indexNames.foreach(indexName => {
            if (multiColIndex.generateName() == indexName) {
              multiIndicesThatAreUsed.put(indexName, multiColIndex)
            }
          })
        })
      })
    })




    // now we have single and double indices.
    // all indices that are used are stored in usedIndices by now.
    // therefore the basis should be the configuration that contains the subsets
    // with the minimal cost



    val seedMultiColConfs = sortAllConfigurationsByCost
//    System.out.println("\n\n\n\n #############\n\n\n seedMultiConfs")
//    seedMultiColConfs.take(5).foreach(bla => System.out.println(bla))
//    val indicesToBeConsidered = new mutable.HashSet[Index]()





    // greedy part of the algorithm - once we've chosen the best conf, greedily add to it
    var bestConf = seedMultiColConfs.head._1
    var bestCost = seedMultiColConfs.head._2
    // generates only single index
    var indicesToBeConsidered = (usedIndices.values.toSet -- bestConf.indexRequests).toList //List(generateIndicesToBeConsidered(bestConf, queries).toList, multiColIndices.toList).flatten

    var isDone = false

    Range(numIndicesSeed, numIndicesTotal).foreach(iteration => {
      if (!isDone) {
        val out = greedyIteration(bestConf, bestCost, queries, indicesToBeConsidered)
        if (out.isEmpty) {
          isDone = true
        } else {
          bestConf = out.get._1
          bestCost = out.get._2
          // indicesToBeConsidered = //generateIndicesToBeConsidered(bestConf, queries).toList
        }
      } else {
        System.out.println("done!")
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


  def calculateRealCost(queries: List[Query]) = {
    Catalog.dropAllHypotheticalIndices()
    val conf = Configuration(Set())

    val res = runHypotheticalAnalyze(conf, queries)
    res.map(run => run.cost).sum
  }

  def generateConfigurationsPerTable(queries: List[Query]) = {
    val table2Index = usedIndices.values.groupBy[Table](indexReq => {indexReq.table})
    val bestConfs = table2Index.map(table2Index => {
      val table = table2Index._1
      val indexLst = table2Index._2

      val (conf, cost) = generateBestConfiguration(table, indexLst, queries)

      (conf, cost)
    })

    val bestConfIndices = mutable.HashSet[CreateIndexRequest]()
    bestConfs.foreach(conf => {
      val c = conf._1
      bestConfIndices.union(c.indexRequests)
    })

    bestConfIndices
  }


  def generateBestConfiguration(table: Table, indexLst: Iterable[CreateIndexRequest], queries: List[Query]) = {
    val res = indexLst.toSet[CreateIndexRequest].subsets.toList.map(subset => {
      val conf = Configuration(subset)
      val runs = runHypotheticalAnalyze(conf, queries)
      val cost = runs.map(run => run.cost).sum

      (conf, cost)
    })

    val out = res.sortBy(c2c => c2c._2)

    out.head
  }


  /**
   * generates multi-column indices as well
   * @return
   */
  def naiveImplementation(numIndices: Int, queries: List[Query]) = {

    val confs = generateConfigurations(numIndices, queries)
    confs.foreach(conf => {
      runHypotheticalAnalyze(conf, queries)
    })
    val out = sortAllConfigurationsByCost

    out
  }


  def generate2ColIndices(queries: List[Query]) = {
    val multiIndex = new mutable.HashSet[CreateIndexRequest]()

    cols2multiIndices.toSet[Column].foreach(col1 => {
      val table = col1.getTable
      table.columns.valuesIterator.foreach(col2 => {
        if (col1 != col2) {
          val indexReq = CreateIndexRequest(table, List(col1, col2), isHypothetical = true)
          val doubleIndexConf = Configuration(Set(indexReq))

          queries.foreach(query => {
            val run = runHypotheticalAnalyze(doubleIndexConf, query)
            if (!run.columns.isEmpty) {
//              System.out.println(s"doubleIndexFound! index=$indexReq")
              multiIndex += indexReq
            }
          })
        }
      })
    })

    multiIndex
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


    System.out.println(s"in GenerateConfs extraCols=${extraColumns.size}")
    // generate indices for extra columns

    val subsets = Range(1, numIndices + 1).map(i => {
      generateSubsets[Column](extraColumns, i)
    }).flatten



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

  def createMultipleIndexRequests(columns: Set[Column], numColumns: Int) = {
    columns.subsets(numColumns).map(subset => {
      CreateIndexRequest(subset.head.getTable, subset.toList, isHypothetical = true)
    })

  }

  def runHypotheticalAnalyze(conf: Configuration, queries: List[Query]) = {
    installConfiguration(conf)

    queries.map(query => {
      val xmlRes = Catalog.runHypotheticalAnalyze(query.sqlQuery)

      val res = AnalyzeRun(query, conf, xmlRes)
      updateState(conf, query, res)
      res
    })
  }

  def runHypotheticalAnalyzeWithooutInstalling(conf: Configuration, query: Query) = {
    val xmlRes = Catalog.runHypotheticalAnalyze(query.sqlQuery)

    val res = AnalyzeRun(query, conf, xmlRes)
    updateState(conf, query, res)
    res
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
    //conf2result
    val lst1 = conf2result.getOrElse(conf, Nil)
    conf2result.update(conf, res :: lst1)

    //query2result
    val lst2 = query2result.getOrElse(query, Nil)
    query2result.update(query, res :: lst2)

    // update columns for future work
    res.columns.foreach(col => cols2multiIndices.add(col))

    res.indexNames.foreach(indexName => {
      if (conf.indexRequests.find(req => req.generateName() == indexName).isDefined) {
        usedIndices.put(indexName, conf.indexRequests.find(req => req.generateName() == indexName).get)
      }
    })


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
