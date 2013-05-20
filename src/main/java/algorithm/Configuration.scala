package algorithm

import database._
import requests.CreateIndexRequest

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/18/13
 * Time: 2:30 PM
 * To change this template use File | Settings | File Templates.
 */
case class Configuration(indexRequests: Set[CreateIndexRequest]) {


  def generateIndices = {
    indexRequests.map(_.generateIndex())
  }

}


object Configuration {
//  def apply(indices: Set[Index]) {
//
//  }
}

