package algorithm

import database.SqlStatement

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/20/13
 * Time: 12:01 AM
 * To change this template use File | Settings | File Templates.
 */
class Query(val sqlStatement: SqlStatement, val args: Any*) {
  val sqlQuery = sqlStatement.getInstantiatedQuery(args)
  var xml = null;

}
