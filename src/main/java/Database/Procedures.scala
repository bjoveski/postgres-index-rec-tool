package database

import algorithm.Query

/**
 * Created with IntelliJ IDEA.
 * User: bjoveski
 * Date: 5/19/13
 * Time: 9:21 AM
 * To change this template use File | Settings | File Templates.
 */
object Procedures {
  // -------
  // util
  val selectAllTables = SqlStatement("SELECT relname FROM pg_stat_all_tables WHERE schemaname='public'")
  val selectAllIndices = SqlStatement("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname!='pg_catalog';")
  val selectColumnNames = SqlStatement("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '?'")
  val selectIndexColumns = SqlStatement(
    """
      |select
      |    t.relname as table_name,
      |    i.relname as index_name,
      |    a.attname as column_name
      |from
      |    pg_class t,
      |    pg_class i,
      |    pg_index ix,
      |    pg_attribute a
      |where
      |    t.oid = ix.indrelid
      |    and i.oid = ix.indexrelid
      |    and a.attrelid = t.oid
      |    and a.attnum = ANY(ix.indkey)
      |    and i.relname = '?'
    """.stripMargin)


  val createRealIndex = SqlStatement("CREATE INDEX ? ON ? (?)")
  val createHypotheticalIndex = SqlStatement("CREATE HYPOTHETICAL INDEX ? ON ? (?)")


  // ------
  // for benchmarks
  val payGetCustSQL = SqlStatement("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, " +
    "c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "+
    "c_balance, c_ytd_payment, c_payment_cnt, c_since FROM customer"  +
    " WHERE c_w_id = ? AND c_d_id = ? AND c_last = '?' ORDER BY c_first")



  // ---------
//  val q1 = new Query("SELECT avg(rating) FROM review WHERE review.i_id=1;")
//  val q2 = new Query("SELECT * FROM review, user WHERE user.u_id = review.u_id AND review.u_id=1 " +
//    "ORDER BY rating LIMIT 10;")



  val tpcc1 = new Query("""SELECT COUNT(DISTINCT (s_i_id)) AS stock_count
                       |  FROM order_line, stock
                       |	WHERE ol_w_id = 3
                       |	AND ol_d_id = 4
                       |	AND ol_o_id <  44
                       |	AND ol_o_id >= 44 - 20
                       |	AND s_w_id =  4
                       |	AND s_i_id = ol_i_id
                       |  AND s_quantity < 100;""".stripMargin,
    (("ol_w_id" :: "ol_d_id" :: "ol_o_id" :: "ol_i_id" :: Nil).map(col => ("order_line", col)) ::
    ("s_w_id" :: "s_i_id" :: "s_quantity" :: Nil).map(col => ("stock", col)) :: Nil).flatten)

  val tpcc2 = new Query("""SELECT o_id, o_carrier_id, o_entry_d FROM oorder
                |			WHERE o_w_id = 3
                |			AND o_d_id = 2 AND o_c_id = 5
                |   ORDER BY o_id DESC LIMIT 1;""".stripMargin,
    List("o_w_id", "o_d_id", "o_id", "o_c_id" ).map(col => ("oorder", col)))
}

