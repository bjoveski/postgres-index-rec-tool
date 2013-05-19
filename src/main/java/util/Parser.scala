package util

/**
 * deals with getting results back from the DB
 */
object Parser {

  def getUsedIndices(xmlElem: xml.Elem) = {
    (xmlElem \\ "Index-Name").map(node => node.text)
  }

  def getTotalCost(xmlElem: xml.Elem) = {
    ((xmlElem \\ "Total-Cost").text).toDouble
  }



}
