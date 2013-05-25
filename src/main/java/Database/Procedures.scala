package database

import algorithm.Query
//import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants

// import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants

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
//  val payGetCustSQL = SqlStatement("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, " +
//    "c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "+
//    "c_balance, c_ytd_payment, c_payment_cnt, c_since FROM customer"  +
//    " WHERE c_w_id = ? AND c_d_id = ? AND c_last = '?' ORDER BY c_first")



  // ---------
//  val q1 = new Query("SELECT avg(rating) FROM review WHERE review.i_id=1;")
//  val q2 = new Query("SELECT * FROM review, user WHERE user.u_id = review.u_id AND review.u_id=1 " +
//    "ORDER BY rating LIMIT 10;")


//
//  val tpcc1 = new Query("""SELECT COUNT(DISTINCT (s_i_id)) AS stock_count
//                       |  FROM order_line, stock
//                       |	WHERE ol_w_id = 3
//                       |	AND ol_d_id = 4
//                       |	AND ol_o_id <  44
//                       |	AND ol_o_id >= 44 - 20
//                       |	AND s_w_id =  4
//                       |	AND s_i_id = ol_i_id
//                       |  AND s_quantity < 100;""".stripMargin,
//    (("ol_w_id" :: "ol_d_id" :: "ol_o_id" :: "ol_i_id" :: Nil).map(col => ("order_line", col)) ::
//    ("s_w_id" :: "s_i_id" :: "s_quantity" :: Nil).map(col => ("stock", col)) :: Nil).flatten)
//
//  val tpcc2 = new Query("""SELECT o_id, o_carrier_id, o_entry_d FROM oorder
//                |			WHERE o_w_id = 3
//                |			AND o_d_id = 2 AND o_c_id = 5
//                |   ORDER BY o_id DESC LIMIT 1;""".stripMargin,
//    List("o_w_id", "o_d_id", "o_id", "o_c_id" ).map(col => ("oorder", col)))
//
//
//
def transform(sqlStmt: SQLStmt): String = {
  sqlStmt.getSQL.replaceAll("\\?", "2")
}


  // delivery

  val delivGetOrderIdSQL = new SQLStmt("SELECT no_o_id FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE no_d_id = ?"
    + " AND no_w_id = ? ORDER BY no_o_id ASC LIMIT 1");
  val delivGetOrderIdSQLq = new Query(transform(delivGetOrderIdSQL),
    List("no_o_id", "no_d_id", "no_w_id", "no_o_id").map(col => (TPCCConstants.TABLENAME_NEWORDER, col)))





  val delivDeleteNewOrderSQL = new SQLStmt("DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER + ""
    + " WHERE no_o_id = ? AND no_d_id = ?"
    + " AND no_w_id = ?");
  val delivDeleteNewOrderSQLq = new Query(transform(delivDeleteNewOrderSQL),
  List("no_o_id", "no_d_id", "no_w_id").map(col => (TPCCConstants.TABLENAME_NEWORDER, col)))






  val delivGetCustIdSQL = new SQLStmt("SELECT o_c_id"
    + " FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE o_id = ?"
    + " AND o_d_id = ?" + " AND o_w_id = ?");
  val delivGetCustIdSQLq = new Query(transform(delivGetCustIdSQL),
  List("o_id", "o_d_id", "o_w_id").map(col => (TPCCConstants.TABLENAME_OPENORDER, col)))





  val delivUpdateCarrierIdSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET o_carrier_id = ?"
    + " WHERE o_id = ?" + " AND o_d_id = ?"
    + " AND o_w_id = ?");
  val delivUpdateCarrierIdSQLq = new Query(transform(delivUpdateCarrierIdSQL),
  List("o_id", "o_d_id", "o_w_id").map(col => (TPCCConstants.TABLENAME_OPENORDER, col)))




  val delivUpdateDeliveryDateSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET ol_delivery_d = '2012/4/23'"
    + " WHERE ol_o_id = ?"
    + " AND ol_d_id = ?"
    + " AND ol_w_id = ?");
  val delivUpdateDeliveryDateSQLq = new Query(transform(delivUpdateDeliveryDateSQL),
  List("ol_o_id", "ol_d_id", "ol_w_id").map(col => (TPCCConstants.TABLENAME_ORDERLINE, col)))




  val delivSumOrderAmountSQL = new SQLStmt("SELECT SUM(ol_amount) AS ol_total"
    + " FROM " + TPCCConstants.TABLENAME_ORDERLINE + "" + " WHERE ol_o_id = ?"
    + " AND ol_d_id = ?" + " AND ol_w_id = ?");
  val delivSumOrderAmountSQLq = new Query(transform(delivSumOrderAmountSQL),
  List("ol_o_id", "ol_d_id", "ol_w_id").map(col => (TPCCConstants.TABLENAME_ORDERLINE, col)))






  val delivUpdateCustBalDelivCntSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET c_balance = c_balance + ?"
    + ", c_delivery_cnt = c_delivery_cnt + 1"
    + " WHERE c_w_id = ?"
    + " AND c_d_id = ?"
    + " AND c_id = ?");
  val delivUpdateCustBalDelivCntSQLq = new Query(transform(delivUpdateCustBalDelivCntSQL),
  List("c_w_id", "c_d_id", "c_id").map(col => (TPCCConstants.TABLENAME_CUSTOMER, col)))




  val delivery = List(delivGetOrderIdSQL, delivDeleteNewOrderSQL, delivGetCustIdSQL,
    delivUpdateCarrierIdSQL, delivUpdateDeliveryDateSQL, delivSumOrderAmountSQL,
    delivUpdateCustBalDelivCntSQL)

  val Adeliveryq = List(delivGetOrderIdSQLq, delivDeleteNewOrderSQLq, delivGetCustIdSQLq,
    delivUpdateCarrierIdSQLq, delivUpdateDeliveryDateSQLq, delivSumOrderAmountSQLq,
    delivUpdateCustBalDelivCntSQLq)














  val ordStatGetNewestOrdSQL = new SQLStmt("SELECT o_id, o_carrier_id, o_entry_d FROM " + TPCCConstants.TABLENAME_OPENORDER
    + " WHERE o_w_id = ?"
    + " AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1");
  val ordStatGetNewestOrdSQLq = new Query(transform(ordStatGetNewestOrdSQL),
  List("o_w_id", "o_d_id", "o_c_id", "o_id").map(col => (TPCCConstants.TABLENAME_OPENORDER, col)))






  val ordStatGetOrderLinesSQL = new SQLStmt("SELECT ol_i_id, ol_supply_w_id, ol_quantity,"
    + " ol_amount, ol_delivery_d"
    + " FROM " + TPCCConstants.TABLENAME_ORDERLINE
    + " WHERE ol_o_id = ?"
    + " AND ol_d_id =?"
    + " AND ol_w_id = ?");
  val ordStatGetOrderLinesSQLq = new Query(transform(ordStatGetOrderLinesSQL),
  List("ol_o_id", "ol_d_id", "ol_w_id").map(col => (TPCCConstants.TABLENAME_ORDERLINE, col)))





  val payGetCustSQL = new SQLStmt("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, "
    + "c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, "
    + "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_since FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE "
    + "c_w_id = ? AND c_d_id = ? AND c_id = ?");
  val payGetCustSQLq = new Query(transform(payGetCustSQL),
  List("c_w_id", "c_d_id", "c_id").map(col => (TPCCConstants.TABLENAME_CUSTOMER, col)))




  val customerByNameSQL = new SQLStmt("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, "
    + "c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "
    + "c_balance, c_ytd_payment, c_payment_cnt, c_since FROM " + TPCCConstants.TABLENAME_CUSTOMER
    + " WHERE c_w_id = ? AND c_d_id = ? AND c_last = 'abc' ORDER BY c_first");
  val customerByNameSQLq = new Query(transform(customerByNameSQL),
  List("c_w_id", "c_d_id", "c_last", "c_first").map(col => (TPCCConstants.TABLENAME_CUSTOMER, col)))



  val orderStatus = List(ordStatGetNewestOrdSQL, ordStatGetOrderLinesSQL,
    payGetCustSQL, customerByNameSQL)

  val AorderStatusq = List(ordStatGetNewestOrdSQLq, ordStatGetOrderLinesSQLq,
    payGetCustSQLq, customerByNameSQLq)






  // payment

  val payUpdateWhseSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + " SET w_ytd = w_ytd + ?  WHERE w_id = ? ");
  val payUpdateWhseSQLq = new Query(transform(payUpdateWhseSQL ),
  List("w_ytd", "w_id").map(col => (TPCCConstants.TABLENAME_WAREHOUSE, col)))



  val payGetWhseSQL = new SQLStmt("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name"
    + " FROM " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE w_id = ?");
  val payGetWhseSQLq = new Query(transform(payGetWhseSQL),
  List("w_id").map(col => (TPCCConstants.TABLENAME_WAREHOUSE, col)))




  val payUpdateDistSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?");
  val payUpdateDistSQLq = new Query(transform(payUpdateDistSQL),
  List("d_ytd", "d_w_id", "d_id").map(col => (TPCCConstants.TABLENAME_DISTRICT, col)))






  val payGetDistSQL = new SQLStmt("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name"
    + " FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE d_w_id = ? AND d_id = ?");
  val payGetDistSQLq = new Query(transform(payGetDistSQL ),
  List("d_w_id", "d_id").map(col => (TPCCConstants.TABLENAME_DISTRICT, col)))


//
//
//  val payGetCustSQL = new SQLStmt("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, "
//    + "c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, "
//    + "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_since FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE "
//    + "c_w_id = ? AND c_d_id = ? AND c_id = ?");
//  val payGetCustSQLq = new Query(transform(payGetCustSQL),
//  List("c_w_id", "c_d_id", "c_id") .map(col => (TPCCConstants.TABLENAME_DISTRICT, col)))



  val payGetCustCdataSQL = new SQLStmt("SELECT c_data FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
  val payGetCustCdataSQLq = new Query(transform(payGetCustCdataSQL ),
  List("c_w_id", "c_d_id", "c_id") .map(col => (TPCCConstants.TABLENAME_CUSTOMER, col)))


  val payUpdateCustBalCdataSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET c_balance = ?, c_ytd_payment = ?, "
    + "c_payment_cnt = ?, c_data = ? "
    + "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
  val payUpdateCustBalCdataSQLq = new Query(transform(payUpdateCustBalCdataSQL ),
  List("c_w_id", "c_d_id", "c_id") .map(col => (TPCCConstants.TABLENAME_CUSTOMER, col)))




  val payUpdateCustBalSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET c_balance = ?, c_ytd_payment = ?, "
    + "c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
  val payUpdateCustBalSQLq = new Query(transform(payUpdateCustBalSQL),
  List("c_w_id", "c_d_id", "c_id").map(col => (TPCCConstants.TABLENAME_CUSTOMER, col)))


  val Apaymentq = List(payUpdateWhseSQLq, payGetWhseSQLq, payUpdateDistSQLq,
    payGetDistSQLq,payGetCustSQLq, payGetCustCdataSQLq, payUpdateCustBalCdataSQLq, payUpdateCustBalSQLq)
//  val payInsertHistSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + " (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) "
//     + " VALUES (?,?,?,?,?,?,?,?)");


//  val customerByNameSQL = new SQLStmt("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, "
//    + "c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "
//    + "c_balance, c_ytd_payment, c_payment_cnt, c_since FROM " + TPCCConstants.TABLENAME_CUSTOMER + " "
//    + "WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first");





}

class SQLStmt(val getSQL:String)

object TPCCConstants {
  val TABLENAME_DISTRICT = "district";
  val TABLENAME_WAREHOUSE = "warehouse";
  val TABLENAME_ITEM = "item";
  val TABLENAME_STOCK = "stock";
  val TABLENAME_CUSTOMER = "customer";
  val TABLENAME_HISTORY = "history";
  val TABLENAME_OPENORDER = "oorder";
  val TABLENAME_ORDERLINE = "order_line";
  val TABLENAME_NEWORDER = "new_order";
}
