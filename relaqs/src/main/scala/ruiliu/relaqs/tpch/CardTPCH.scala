/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package ruiliu.relaqs.tpch

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class CardTPCH (bootstrap: String,
                query: String,
                numBatch: Int,
                shuffleNum: String,
                statDIR: String,
                SF: Double,
                hdfsRoot: String,
                execution_mode: String,
                inputPartitions: Int,
                constraint: String,
                largeDataset: Boolean,
                iOLAPConf: Int,
                iOLAPRoot: String,
                SR: Double) {
  val iOLAP_Q11_src = "/q11_config.csv"
  val iOLAP_Q17_src = "/q17_config.csv"
  val iOLAP_Q18_src = "/q18_config.csv"
  val iOLAP_Q20_src = "/q20_config.csv"
  val iOLAP_Q22_src = "/q22_config.csv"

  val iOLAP_Q11_dst = "/iOLAP/q11_config.dst"
  val iOLAP_Q17_dst = "/iOLAP/q17_config.dst"
  val iOLAP_Q18_dst = "/iOLAP/q18_config.dst"
  val iOLAP_Q20_dst = "/iOLAP/q20_config.dst"
  val iOLAP_Q22_dst = "/iOLAP/q22_config.dst"

  val iOLAP_ON = 0
  val iOLAP_OFF = 1
  val iOLAP_TRAINING = 2

  DataUtils.bootstrap = bootstrap
  TPCHSchema.setQueryMetaData(numBatch, SF, SR, hdfsRoot, inputPartitions, largeDataset)

  private var query_name: String = null

  val enable_iOLAP =
    if (iOLAPConf == iOLAP_ON) "true"
    else "false"

  def execQuery(query: String): Unit = {
    query_name = query.toLowerCase

    val sparkConf = new SparkConf()
      .set(SQLConf.SHUFFLE_PARTITIONS.key, shuffleNum)
      .set(SQLConf.SLOTHDB_STAT_DIR.key, statDIR)
      .set(SQLConf.SLOTHDB_EXECUTION_MODE.key, execution_mode)
      .set(SQLConf.SLOTHDB_BATCH_NUM.key, numBatch.toString)
      .set(SQLConf.SLOTHDB_IOLAP.key, enable_iOLAP)
      .set(SQLConf.SLOTHDB_QUERYNAME.key, query_name)

    val digit_constraint = constraint.toDouble
    if (digit_constraint <= 1.0) sparkConf.set(SQLConf.SLOTHDB_LATENCY_CONSTRAINT.key, constraint)
    else sparkConf.set(SQLConf.SLOTHDB_RESOURCE_CONSTRAINT.key, constraint)

    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName("Executing Query " + query)
      .getOrCreate()

    query_name match {
      case "q1" =>
        execQ1(spark)
      case "q2" =>
        execQ2(spark)
      case "q3" =>
        execQ3(spark)
      case "q4" =>
        execQ4(spark)
      case "q5" =>
        execQ5(spark)
      case "q6" =>
        execQ6(spark)
      case "q7" =>
        execQ7(spark)
      case "q8" =>
        execQ8(spark)
      case "q9" =>
        execQ9(spark)
      case "q10" =>
        execQ10(spark)
      case "q11" =>
        execQ11(spark)
      case "q12" =>
        execQ12(spark)
      case "q13" =>
        execQ13(spark)
      case "q14" =>
        execQ14(spark)
      case "q15" =>
        execQ15(spark)
      case "q16" =>
        execQ16(spark)
      case "q17" =>
        execQ17(spark)
      case "q18" =>
        execQ18(spark)
      case "q19" =>
        execQ19(spark)
      case "q20" =>
        execQ20(spark)
      case "q21" =>
        execQ21(spark)
      case "q22" =>
        execQ22(spark)
      case "q_highbalance" =>
        execHighBalance(spark)
      case "q_scan" =>
        execScan(spark)
      case "q_static" =>
        execStatic(spark)
      case _ =>
        printf("Not yet supported %s\n", query)
    }
  }

  def execQ1(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_qty = new DoubleSum
    val sum_base_price = new DoubleSum
    val sum_disc_price = new Sum_disc_price
    val sum_charge = new Sum_disc_price_with_tax
    val avg_qty = new DoubleAvg
    val avg_price = new DoubleAvg
    val avg_disc = new DoubleAvg
    val count_order = new Count

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")

    val result = l.filter($"l_shipdate" <= "1998-09-01")
      .select($"l_returnflag", $"l_linestatus",
        $"l_quantity", $"l_extendedprice", $"l_discount", $"l_tax")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum_qty($"l_quantity").as("sum_qty"),
        sum_base_price($"l_extendedprice" * $"l_discount").as("sum_base_price"),
        sum_disc_price($"l_extendedprice", $"l_discount").as("sum_disc_price"),
        sum_charge($"l_extendedprice", $"l_discount", $"l_tax").as("sum_charge"),
        avg_qty($"l_quantity").as("avg_qty"),
        avg_price($"l_extendedprice").as("avg_price"),
        avg_disc($"l_discount").as("avg_disc"),
        count_order(lit(1L)).as("count_order")
      )
      // .orderBy($"l_returnflag", $"l_linestatus")

    // result.explain(true)

    DataUtils.writeToSink(result, query_name)
  }

  def execQ2_subquery(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
    val r = DataUtils.loadStreamTable(spark, "region", "r")
      .filter($"r_name" === "EUROPE")

    return r.join(n, $"r_regionkey" === $"n_regionkey")
      .join(s, $"n_nationkey" === $"s_nationkey")
      .join(ps, $"s_suppkey" === $"ps_suppkey")
      .groupBy($"ps_partkey")
      .agg(
        min($"ps_supplycost").as("min_supplycost"))
      .select($"ps_partkey".as("min_partkey"), $"min_supplycost")
  }

  def execQ2(spark: SparkSession): Unit = {
    import spark.implicits._

    val p = DataUtils.loadStreamTable(spark, "part", "p")
      .filter(($"p_size" === 15) and ($"p_type" like("%BRASS")))
    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val n = DataUtils.loadStreamTable(spark, "nation", "n")
    val r = DataUtils.loadStreamTable(spark, "region", "r")
      .filter($"r_name" === "EUROPE")

    val subquery1_a = r.join(n, $"r_regionkey" === $"n_regionkey")
      .join(s, $"n_nationkey" === $"s_nationkey")

    val subquery1_b = ps.join(p, $"ps_partkey" === $"p_partkey")
    val subquery1 = subquery1_a.join(subquery1_b, $"s_suppkey" === $"ps_suppkey")

    val subquery2 = execQ2_subquery(spark)

    val result = subquery1
      .join(subquery2, ($"p_partkey" ===  $"min_partkey")
        and ($"ps_supplycost" === $"min_supplycost"))
      // .orderBy(desc("s_acctbal"), $"n_name", $"s_name", $"p_partkey")
      .select($"s_acctbal", $"s_name", $"n_name",
        $"p_partkey", $"p_mfgr", $"s_address", $"s_phone", $"s_comment")
      // .limit(100)

    DataUtils.writeToSink(result, query_name)
  }

  def execQ3(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
      .filter($"c_mktsegment" === "BUILDING")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" < "1995-03-15")
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" > "1995-03-15")

    val result = c.join(o, $"c_custkey" === $"o_custkey")
      .join(l, $"o_orderkey" === $"l_orderkey")
      .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
      .agg(
        sum_disc_price($"l_extendedprice", $"l_discount").alias("revenue"))
      // .orderBy(desc("revenue"), $"o_orderdate")
      .select("l_orderkey", "revenue", "o_orderdate", "o_shippriority")
      // .limit(10)

    // result.explain(false)

    DataUtils.writeToSink(result, query_name)
  }

  def execQ4(spark: SparkSession): Unit = {
    import spark.implicits._

    val order_count = new Count

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" >= "1993-07-01"
      and $"o_orderdate" < "1993-10-01")

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_commitdate" < $"l_receiptdate")
      .select("l_orderkey")

    val result = o.join(l, $"o_orderkey" === $"l_orderkey", "left_semi")
      .groupBy("o_orderpriority")
      .agg(
        order_count(lit(1)).alias("order_count"))
    //  .orderBy("o_orderpriority")

    // result.explain(false)

    DataUtils.writeToSink(result, query_name)
  }

  def execQ5(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" >= "1994-01-01" and $"o_orderdate" < "1995-01-01")
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val s = DataUtils.loadStreamTable(spark, "supplier", "s")

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
    val r = DataUtils.loadStreamTable(spark, "region", "r")
      .filter($"r_name" === "ASIA")

    val query_a = r.join(n, $"r_regionkey" === $"n_regionkey")
      .join(s, $"n_nationkey" === $"s_nationkey")

    val query_b = l.join(o, $"l_orderkey" === $"o_orderkey")
      .join(c, $"o_custkey" === $"c_custkey")

    val result = query_a.join(query_b, $"s_nationkey" === $"c_nationkey"
      and $"s_suppkey" === $"l_suppkey")
      .groupBy("n_name")
      .agg(
        sum_disc_price($"l_extendedprice", $"l_discount" ).alias("revenue"))
    //  .orderBy(desc("revenue"))

    // result.explain(true)

    DataUtils.writeToSink(result, query_name)
  }

  def execQ6(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipdate" between("1994-01-01", "1995-01-01"))
        and ($"l_discount" between(0.05, 0.07)) and ($"l_quantity" < 24))

    val result = l.agg(
      doubleSum($"l_extendedprice" * $"l_discount").alias("revenue")
    )

    // result.explain(true)
    DataUtils.writeToSink(result, query_name)
  }

  def execQ7(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1995-01-01", "1996-12-31"))
    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")
    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val n1 = DataUtils.loadStreamTable(spark, "nation", "n1")
      .select($"n_name".alias("supp_nation"), $"n_nationkey".as("n1_nationkey"))
    val n2 = DataUtils.loadStreamTable(spark, "nation", "n2")
      .select($"n_name".alias("cust_nation"), $"n_nationkey".as("n2_nationkey"))

    val result = l.join(s, $"l_suppkey" === $"s_suppkey")
      .join(o, $"l_orderkey" === $"o_orderkey")
      .join(c, $"o_custkey" === $"c_custkey")
      .join(n1, $"s_nationkey" === $"n1_nationkey")
      .join(n2, $"c_nationkey" === $"n2_nationkey")
      .filter(($"supp_nation" === "FRANCE" and $"cust_nation" === "GERMANY")
        or ($"supp_nation" === "GERMANY" and $"cust_nation" === "FRANCE"))
      .select($"supp_nation", $"cust_nation", year($"l_shipdate").as("l_year"),
        $"l_extendedprice", $"l_discount")
      .groupBy("supp_nation", "cust_nation", "l_year")
      .agg(
        sum_disc_price($"l_extendedprice", $"l_discount").as("revenue"))
    //  .orderBy("supp_nation", "cust_nation", "l_year")

    // result.explain(true)
    DataUtils.writeToSink(result, query_name)
  }

  def execQ8(spark: SparkSession): Unit = {
    import spark.implicits._

    val udaf_q8 = new UDAF_Q8

    val p = DataUtils.loadStreamTable(spark, "part", "p")
      .filter($"p_type" === "ECONOMY ANODIZED STEEL")
    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" between("1995-01-01", "1996-12-31"))
    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val n1 = DataUtils.loadStreamTable(spark, "nation", "n1")
      .select($"n_regionkey".alias("n1_regionkey"), $"n_nationkey".as("n1_nationkey"))
    val n2 = DataUtils.loadStreamTable(spark, "nation", "n2")
      .select($"n_name".alias("n2_name"), $"n_nationkey".as("n2_nationkey"))
    val r = DataUtils.loadStreamTable(spark, "region", "r")
      .filter($"r_name" === "AMERICA")

    val result = l.join(p, $"l_partkey" === $"p_partkey")
      .join(s, $"l_suppkey" === $"s_suppkey")
      .join(o, $"l_orderkey" === $"o_orderkey")
      .join(c, $"o_custkey" === $"c_custkey")
      .join(n1, $"c_nationkey" === $"n1_nationkey")
      .join(r, $"n1_regionkey" === $"r_regionkey")
      .join(n2, $"s_nationkey" === $"n2_nationkey")
      .select(year($"o_orderdate").as("o_year"),
        ($"l_extendedprice" * ($"l_discount" - 1) * -1).as("volume"), $"n2_name")
      .groupBy($"o_year")
      .agg(udaf_q8($"n2_name", $"volume").as("mkt_share"))
    //  .orderBy($"o_year")

    // result.explain(true)
    DataUtils.writeToSink(result, query_name)
  }

  def execQ9(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum

    val p = DataUtils.loadStreamTable(spark, "part", "p").filter($"p_name" like("%green%"))
    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")
    val n = DataUtils.loadStreamTable(spark, "nation", "n")

    val result = l.join(p, $"l_partkey" === $"p_partkey")
      .join(ps, $"l_partkey" === $"ps_partkey" and $"l_suppkey" === $"ps_suppkey")
      .join(s, $"l_suppkey" === $"s_suppkey")
      .join(o, $"l_orderkey" === $"o_orderkey")
      .join(n, $"s_nationkey" === $"n_nationkey")
      .select($"n_name".as("nation"),
        year($"o_orderdate").as("o_year"),
        (($"l_extendedprice" * ($"l_discount" - 1) * -1) - $"ps_supplycost" * $"l_quantity")
          .as("amount"))
      .groupBy("nation", "o_year")
      .agg(
        doubleSum($"amount").as("sum_profit"))
    //  .orderBy($"nation", desc("o_year"))

    // result.explain(true)
    DataUtils.writeToSink(result, query_name)
  }

  def execQ10(spark: SparkSession): Unit = {
    import spark.implicits._

    val revenue = new Sum_disc_price

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" >= "1993-10-01" and $"o_orderdate" < "1994-01-01")
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_returnflag" === "R")
    val n = DataUtils.loadStreamTable(spark, "nation", "n")

    val result = l.join(o, $"l_orderkey" === $"o_orderkey")
      .join(c, $"o_custkey" === $"c_custkey")
      .join(n, $"c_nationkey" === $"n_nationkey")
      .groupBy("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment")
      .agg(
        revenue($"l_extendedprice", $"l_discount").as("revenue"))
    //  .orderBy(desc("revenue"))

    // result.explain(true)
    DataUtils.writeToSink(result, query_name)
  }

  def execQ11_subquery(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val doubleSum = new DoubleSum

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val n = DataUtils.loadStreamTable(spark, "nation", "n")
      .filter($"n_name" === "GERMANY")

    return s.join(n, $"s_nationkey" === $"n_nationkey")
      .join(ps, $"s_suppkey" === $"ps_suppkey")
      .agg(
        doubleSum($"ps_supplycost" * $"ps_availqty" * 0.0001/SF).as("small_value"))
  }

  def execQ11(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val n = DataUtils.loadStreamTable(spark, "nation", "n")
      .filter($"n_name" === "GERMANY")

    val subquery = execQ11_subquery(spark)

    if (iOLAPConf == iOLAP_TRAINING) {
      DataUtils.writeToSink(subquery.agg(min($"small_value"), max($"small_value")), query_name)
    } else {
      val result =
        s.join(n, $"s_nationkey" === $"n_nationkey")
          .join(ps, $"s_suppkey" === $"ps_suppkey")
          .groupBy($"ps_partkey")
          .agg(
            doubleSum($"ps_supplycost" * $"ps_availqty").as("value"))
          .join(subquery, $"value" > $"small_value", "cross")
          .select($"ps_partkey", $"value")

      // .orderBy(desc("value"))
      // result.explain()
      DataUtils.writeToSink(result, query_name)
    }
  }

  def execQ12(spark: SparkSession): Unit = {
    import spark.implicits._

    val udaf_q12_low = new UDAF_Q12_LOW
    val udaf_q12_high = new UDAF_Q12_HIGH

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipmode" === "MAIL")
        and ($"l_commitdate" < $"l_receiptdate")
        and ($"l_shipdate" < $"l_commitdate")
        and ($"l_receiptdate" === "1994-01-01"))

    val result = o.join(l, $"o_orderkey" === $"l_orderkey")
      .groupBy($"l_shipmode")
      .agg(
          udaf_q12_high($"o_orderpriority").as("high_line_count"),
          udaf_q12_low($"o_orderpriority").as("low_line_count"))
      // .orderBy($"l_shipmode")

    // result.explain(true)

    DataUtils.writeToSink(result, query_name)
  }

  def execQ13(spark: SparkSession): Unit = {
    import spark.implicits._

    val c_count = new Count_not_null
    val custdist = new Count

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter(!($"o_comment" like("%special%requests%")))

    val result = c.join(o, $"c_custkey" === $"o_custkey", "left_outer")
      .groupBy($"c_custkey")
      .agg(
        c_count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(
        custdist(lit(1)).as("custdist"))
    //  .orderBy(desc("custdist"), desc("c_count"))

    // result.explain(true)

    DataUtils.writeToSink(result, query_name)
  }

  def execQ14(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val udaf_q14 = new UDAF_Q14
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1995-09-01", "1995-10-01"))
    val p = DataUtils.loadStreamTable(spark, "part", "p")

    val result = l.join(p, $"l_partkey" === $"p_partkey")
      .agg(
        ((udaf_q14($"p_type", $"l_extendedprice", $"l_discount")/
        sum_disc_price($"l_extendedprice", $"l_discount")) * 100).as("promo_revenue"))

    // result.explain(true)
    DataUtils.writeToSink(result, query_name)
  }

  def execQ15_subquery(spark: SparkSession): DataFrame = {
    import  spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1996-01-01", "1996-04-01"))

    return l.groupBy($"l_suppkey")
      .agg(
       sum_disc_price($"l_extendedprice", $"l_discount").as("total_revenue"))
      .select($"l_suppkey".as("supplier_no"), $"total_revenue")
  }

  def execQ15(spark: SparkSession): Unit = {
    import spark.implicits._

    val count = new Count

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val revenue = execQ15_subquery(spark)
    val max_revenue = execQ15_subquery(spark).agg(max($"total_revenue").as("max_revenue"))

    // val result = revenue.join(max_revenue, $"total_revenue" === $"max_revenue")
    //     .select($"supplier_no", $"total_revenue")
    val result = s.join(revenue, $"s_suppkey" === $"supplier_no")
     .join(max_revenue, $"total_revenue" >= $"max_revenue", "cross")
     .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
    // .orderBy("s_suppkey")

    // result.explain(true)
    DataUtils.writeToSink(result, query_name)
  }

  def execQ16(spark: SparkSession): Unit = {
    import spark.implicits._

    val supplier_cnt = new Count

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")

    val p = DataUtils.loadStreamTable(spark, "part", "part")
      .filter(($"p_brand" =!= "Brand#45") and
        (!($"p_type" like("MEDIUM POLISHED%")))
        and ($"p_size" isin(49, 14, 23, 45, 19, 3, 36, 9)))

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
      .filter($"s_comment" like("%Customer%Complaints%"))
      .select($"s_suppkey")

    val result = ps.join(s, $"ps_suppkey" === $"s_suppkey", "left_anti")
      .join(p, $"ps_partkey" === $"p_partkey")
      .select($"p_brand", $"p_type", $"p_size", $"ps_suppkey")
      // .dropDuplicates()
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(supplier_cnt($"ps_suppkey").as("supplier_cnt"))
    //  .orderBy(desc("supplier_cnt"), $"p_brand", $"p_type", $"p_size")

    // result.explain(true)
    DataUtils.writeToSink(result, query_name)
  }

  def execQ17(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val doubleSum = new DoubleSum

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val p = DataUtils.loadStreamTable(spark, "part", "p")
      .filter($"p_brand" === "Brand#23" and $"p_container" === "MED BOX")

    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .groupBy($"l_partkey")
      .agg((doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
      .select($"l_partkey".as("agg_l_partkey"), $"avg_quantity")

    if (iOLAPConf == iOLAP_TRAINING) {
      val tmpDF = l.join(agg_l, $"l_partkey" === $"agg_l_partkey"
        and $"l_quantity" < $"avg_quantity").select($"l_partkey")
        .dropDuplicates()

      val fullP = DataUtils.loadStreamTable(spark, "part", "p")
      val result = fullP.join(tmpDF, $"p_partkey" === $"l_partkey", "left_anti")
          .select($"p_partkey")

      DataUtils.writeToFile(result, query_name, hdfsRoot + iOLAP_Q17_dst)

    } else {
      val result =
        p.join(agg_l, $"p_partkey" === $"agg_l_partkey")
            .join(l, $"agg_l_partkey" === $"l_partkey"
              and $"avg_quantity" > $"l_quantity")
            .agg((doubleSum($"l_extendedprice") / 7.0).as("avg_yearly"))

      DataUtils.writeToSink(result, query_name)

    }
  }

  def execQ18(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleSum1 = new DoubleSum
    val doubleSum2 = new DoubleSum

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .groupBy("l_orderkey")
      .agg(doubleSum1($"l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("agg_orderkey"))

    if (iOLAPConf == iOLAP_TRAINING) {
       DataUtils.writeToFile(agg_l, query_name, hdfsRoot + iOLAP_Q18_dst)
    } else {
      val result =
        o.join(agg_l, $"o_orderkey" === $"agg_orderkey", "left_semi")
            .join(c, $"o_custkey" === $"c_custkey")
            .join(l, $"o_orderkey" === $"l_orderkey")
            .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
            .agg(doubleSum2($"l_quantity"))

        // if (iOLAPConf == iOLAP_ON) {
        //   val keyArray = DataUtils.loadIOLAPLongTable(spark, iOLAPRoot + iOLAP_Q18_src)

        //   o.filter($"o_orderkey".isInCollection(keyArray))
        //     .join(agg_l, $"o_orderkey" === $"agg_orderkey", "left_semi")
        //     .join(l, $"o_orderkey" === $"l_orderkey")
        //     .join(c, $"o_custkey" === $"c_custkey")
        //     .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
        //     .agg(doubleSum2($"l_quantity"))
        // } else {
        // }
      //  .orderBy(desc("o_totalprice"), $"o_orderdate")

      // result.explain(true)
      DataUtils.writeToSink(result, query_name)
    }
  }

  def execQ19(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipmode" isin("AIR", "AIR REG"))
        and ($"l_shipinstruct" === "DELIVER IN PERSON"))
    val p = DataUtils.loadStreamTable(spark, "part", "p")

    val result = l.join(p, $"l_partkey" === $"p_partkey"
      and ((($"p_brand" === "Brand#12") and
      ($"p_container" isin("SM CASE", "SM BOX", "SM PACK", "SM PKG")) and
      ($"l_quantity" >= 1 and $"l_quantity" <= 11) and
      ($"p_size" between(1, 5))
       )
       or (($"p_brand" === "Brand#23") and
      ($"p_container" isin("MED BAG", "MED BOX", "MED PKG", "MED PACK")) and
      ($"l_quantity" >= 10 and $"l_quantity" <= 20) and
      ($"p_size" between(1, 10))
       )
       or (($"p_brand" === "Brand#34") and
      ($"p_container" isin("LG CASE", "LG BOX", "LG PACK", "LG PKG")) and
      ($"l_quantity" >= 20 and $"l_quantity" <= 30) and
      ($"p_size" between(1, 15))))
      )
      .agg(sum_disc_price($"l_extendedprice", $"l_discount").as("revenue"))

    // result.explain(true)

    DataUtils.writeToSink(result, query_name)
  }

  def execQ20(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum

    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1994-01-01", "1994-12-31"))
      .groupBy($"l_partkey", $"l_suppkey")
      .agg((doubleSum($"l_quantity") * 0.5).as("agg_l_sum"))
      .select($"l_partkey".as("agg_l_partkey"),
      $"l_suppkey".as("agg_l_suppkey"),
      $"agg_l_sum")

    val p = DataUtils.loadStreamTable(spark, "part", "p")
      .filter($"p_name" like("forest%"))

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")

    val subquery = ps.join(agg_l, $"ps_partkey" === $"agg_l_partkey"
        and $"ps_suppkey" === $"agg_l_suppkey" and $"ps_availqty" > $"agg_l_sum")
      .join(p, $"ps_partkey" === $"p_partkey", "left_semi")
      .select("ps_suppkey")

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val n = DataUtils.loadStreamTable(spark, "nation", "n")
      .filter($"n_name" === "CANADA")

    if (iOLAPConf == iOLAP_TRAINING) {
      DataUtils.writeToFile(subquery, query_name, hdfsRoot + iOLAP_Q20_dst)
    } else {
      val result =
        s.join(n, $"s_nationkey" === $"n_nationkey")
          .join(subquery, $"s_suppkey" === $"ps_suppkey", "left_semi")
          .select($"s_name", $"s_address")
        // if (iOLAPConf == iOLAP_ON) {
        //   val keyArray = DataUtils.loadIOLAPLongTable(spark, iOLAPRoot + iOLAP_Q20_src)
        //   s.filter($"s_suppkey".isInCollection(keyArray))
        //     .join(subquery, $"s_suppkey" === $"ps_suppkey", "left_semi")
        //     .join(n, $"s_nationkey" === $"n_nationkey")
        //     .select($"s_name", $"s_address")
        // } else {
        // }

      DataUtils.writeToSink(result, query_name)
    }
  }

  def execQ21(spark: SparkSession): Unit = {
    import spark.implicits._

    val count = new Count

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val l1 = DataUtils.loadStreamTable(spark, "lineitem", "l1")
      .filter($"l_receiptdate" > $"l_commitdate")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderstatus" === "F")
    val n = DataUtils.loadStreamTable(spark, "nation", "n")
      .filter($"n_name" === "SAUDI ARABIA")

    val init_result = l1.join(s, $"l_suppkey" === $"s_suppkey")
      .join(n, $"s_nationkey" === $"n_nationkey")

    val l2 = DataUtils.loadStreamTable(spark, "lineitem", "l2")
      .select($"l_orderkey".as("l2_orderkey"),
      $"l_suppkey".as("l2_suppkey"))
    val l3 = DataUtils.loadStreamTable(spark, "lineitem", "l3")
      .filter($"l_receiptdate" > $"l_commitdate")
      .select($"l_orderkey".as("l3_orderkey"),
      $"l_suppkey".as("l3_suppkey"))

    val result = init_result
      .join(l2, ($"l_orderkey" === $"l2_orderkey")
        and ($"l_suppkey" =!= $"l2_suppkey"), "left_semi")
      .join(o, $"l_orderkey" === $"o_orderkey")
      .join(l3, ($"l_orderkey" === $"l3_orderkey")
        and ($"l_suppkey" =!= $"l3_suppkey"), "left_anti")
      .groupBy("s_name")
      .agg(count(lit(1)).as("numwait"))
    //  .orderBy(desc("numwait"), $"s_name")

    // result.explain(true)

    DataUtils.writeToSink(result, query_name)
  }

  def execQ22(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val numcust = new Count
    val doubleSum = new DoubleSum

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
      .filter(substring($"c_phone", 1, 2)
        isin("13", "31", "23", "29", "30", "18", "17"))

    val subquery1 = DataUtils.loadStreamTable(spark, "customer", "c1")
      .filter((substring($"c_phone", 1, 2)
        isin("13", "31", "23", "29", "30", "18", "17")) and
        ($"c_acctbal" > 0.00))
      .agg(doubleAvg($"c_acctbal").as("avg_acctbal"))

    val o = DataUtils.loadStreamTable(spark, "orders", "o")

    if (iOLAPConf == iOLAP_TRAINING) {
      DataUtils.writeToSink(subquery1.agg(min($"avg_acctbal"), max($"avg_acctbal")), query_name)
    } else {
      val result =
        c.join(o, $"c_custkey" === $"o_custkey", "left_anti")
          .join(subquery1, $"c_acctbal" > $"avg_acctbal", "cross")
          .select(substring($"c_phone", 1, 2).as("cntrycode"), $"c_acctbal")
          .groupBy($"cntrycode")
          .agg(numcust(lit(1)).as("numcust"),
            doubleSum($"c_acctbal").as("totalacctbal"))

        // if (iOLAPConf == iOLAP_ON) {
        //   val bal = DataUtils.loadIOLAPDoubleTable(spark, iOLAPRoot + iOLAP_Q22_src)

        //   c.filter($"c_acctbal" > bal)
        //     .join(o, $"c_custkey" === $"o_custkey", "left_anti")
        //     .join(subquery1, $"c_acctbal" > $"avg_acctbal", "cross")
        //     .select(substring($"c_phone", 1, 2).as("cntrycode"), $"c_acctbal")
        //     .groupBy($"cntrycode")
        //     .agg(numcust(lit(1)).as("numcust"),
        //       doubleSum($"c_acctbal").as("totalacctbal"))
        // } else {
        // }

       // .orderBy($"cntrycode")
       // result.explain(true)
      DataUtils.writeToSink(result, query_name)
    }
  }

  def execHighBalance(spark: SparkSession): Unit = {
    import spark.implicits._

    val avgBal = new DoubleAvg
    val agg_c = DataUtils.loadStreamTable(spark, "customer", "c")
      .agg(avgBal($"c_acctbal").as("avg_bal"))

    val count = new Count
    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val result = c.join(agg_c, $"c_acctbal" > $"avg_bal", "cross")
        .agg(count(lit(1L)).as("high balance customer"))

    DataUtils.writeToSink(result, query_name)
  }

  def execScan(spark: SparkSession): Unit = {
    import spark.implicits._

    val result = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" === "2000-09-01")

    DataUtils.writeToSink(result, query_name)
  }

  def execStatic(spark: SparkSession): Unit = {
    import spark.implicits._

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val r = DataUtils.loadStaticTable(spark, "region", "r")
    val n = DataUtils.loadStaticTable(spark, "nation", "n")
    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")

    val result = r.join(n, $"r_regionkey" === $"n_regionkey")
      .join(s, $"n_nationkey" === $"s_nationkey")
      .join(ps, $"s_suppkey" === $"ps_suppkey")
      .groupBy($"ps_partkey")
      .agg(
        min($"ps_supplycost").as("min_supplycost"))
      .select($"ps_partkey".as("min_partkey"), $"min_supplycost")

    result.explain()
    // DataUtils.writeToSink(result, "q_static")

  }
}

object CardTPCH {
  def main(args: Array[String]): Unit = {

    if (args.length < 13) {
      System.err.println("Usage: CardTPCH <bootstrap-servers> <query>" +
        "<numBatch> <number-shuffle-partition> <statistics dir> <SF> <HDFS root>" +
        "<execution mode [0: inc-aware(subpath), 1: inc-aware(subplan), 2: inc-oblivious, " +
        "3: generate inc statistics, 4: training]>  <num of input partitions>" +
        "<performance constraint> <large dataset> <iOLAP Config> <iOLAP root> <sample rate>")
      System.exit(1)
    }

    val tpch = new CardTPCH(args(0), args(1), args(2).toInt, args(3),
      args(4), args(5).toDouble, args(6), args(7), args(8).toInt, args(9),
      args(10).toBoolean, args(11).toInt, args(12), args(13).toDouble)
    tpch.execQuery(args(1))
  }
}

// scalastyle:off println
