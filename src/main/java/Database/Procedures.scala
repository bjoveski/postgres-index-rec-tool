package database

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


}
