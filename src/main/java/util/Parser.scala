package util

/**
 * deals with getting results back from the DB
 */
object Parser {

  def getUsedIndexNames(xmlElem: xml.Elem) = {
    (xmlElem \\ "Index-Name").map(node => node.text)
  }

  def getTotalCost(xmlElem: xml.Elem) = {

    (xmlElem \"Query" \ "Plan" \ "Total-Cost").text.toDouble
  }



}
