package database

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/18/13
 * Time: 7:02 PM
 * To change this template use File | Settings | File Templates.
 */
case class SqlStatement(templateQuery: String) {

  var sampleQuery: String = ""

  def getInstantiatedQuery(args: Any*) = {

    val parts = templateQuery.split("\\?")
    val builder = new StringBuilder()
    builder.append(parts.head)

    if (parts.size != args.size + 1) {
      throw new IllegalArgumentException("number of params is not right")
    }

    args.zip(parts.tail).foreach(tuple => {
      builder.append(tuple._1)
      builder.append(tuple._2)
    })

    builder.toString()
  }

  override def toString = {
    templateQuery
  }
}

object SqlStatement {
  // sets sample query
  def apply(templateQuery: String, args: Any*) = {
    val res = new SqlStatement(templateQuery)
    res.sampleQuery = res.getInstantiatedQuery(args)
    res
  }
}


