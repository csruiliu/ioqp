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
package ruiliu.aqp.tpch

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SlothDBContext

//noinspection ScalaStyle
class QueryTPCH(bootstrap: String,
                query: String,
                numBatch: Int,
                shuffleNum: String,
                statDIR: String,
                staticDIR: String,
                SF: Double,
                hdfsRoot: String,
                execution_mode: String,
                inputPartitions: Int,
                constraint: String,
                largeDataset: Boolean,
                iOLAPConf: Int,
                incPercentage: String,
                costBias: String,
                maxStep: String,
                sampleTime: String,
                SR: Double,
                trigger_interval: Int,
                aggregation_interval: Int,
                checkpoint_path: String,
                cbo_enable: String) {

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

  val iOLAP_OFF = 0
  val iOLAP_ON = 1
  val iOLAP_TRAINING = 2

  DataUtils.bootstrap = bootstrap

  TPCHSchema.setQueryMetaData(numBatch, SF, SR, hdfsRoot, staticDIR,
    inputPartitions, largeDataset, checkpoint_path)

  if (checkpoint_path == "none") {
    SlothDBContext.enable_checkpoint = false
  }
  else {
    SlothDBContext.enable_checkpoint = true
  }

  printf("Checkpoint Path: %s\n", TPCHSchema.checkpointPath)
  printf("Sample Rate: %f\n", SR)
  printf("Aggregation Interval: %d\n", aggregation_interval)

  private var query_name: String = null

  val enable_iOLAP: String = if (iOLAPConf == iOLAP_ON) "true" else "false"

  def execQuery(): Unit = {
    query_name = query.toLowerCase

    val sparkConf = new SparkConf()
      .set(SQLConf.CBO_ENABLED.key, cbo_enable)
      .set(SQLConf.SHUFFLE_PARTITIONS.key, shuffleNum)
      .set(SQLConf.SLOTHDB_STAT_DIR.key, statDIR)
      .set(SQLConf.SLOTHDB_EXECUTION_MODE.key, execution_mode)
      .set(SQLConf.SLOTHDB_BATCH_NUM.key, numBatch.toString)
      .set(SQLConf.SLOTHDB_IOLAP.key, enable_iOLAP)
      .set(SQLConf.SLOTHDB_QUERYNAME.key, query_name)
      .set(SQLConf.SLOTHDB_INC_PERCENTAGE.key, incPercentage)
      .set(SQLConf.SLOTHDB_COST_MODEL_BIAS.key, costBias)
      .set(SQLConf.SLOTHDB_MAX_STEP.key, maxStep)
      .set(SQLConf.SLOTHDB_SAMPLE_TIME.key, sampleTime)

    val digit_constraint = constraint.toDouble
    if (digit_constraint <= 1.0) sparkConf.set(SQLConf.SLOTHDB_LATENCY_CONSTRAINT.key, constraint)
    else sparkConf.set(SQLConf.SLOTHDB_RESOURCE_CONSTRAINT.key, constraint)

    // set checkpoint location
    // sparkConf.set(SQLConf.CHECKPOINT_LOCATION.key, TPCHSchema.checkpointPath + "/" + query_name)

    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName("Executing Query " + query_name)
      .getOrCreate()

    printf("APPID:%s\n", spark.sparkContext.applicationId)
    // spark.sparkContext.setCheckpointDir(TPCHSchema.checkpointPath + "/" + query_name)
    val query_name_prefix = query_name.split("_")(0)

    query_name_prefix match {
      case "q1" => execQ1(spark)
      case "q2" => execQ2(spark)
      case "q3" => execQ3(spark)
      case "q4" => execQ4(spark)
      case "q5" => execQ5(spark)
      case "q6" => execQ6(spark)
      case "q7" => execQ7(spark)
      case "q8" => execQ8(spark)
      case "q9" => execQ9(spark)
      case "q10" => execQ10(spark)
      case "q11" => execQ11(spark)
      case "q12" => execQ12(spark)
      case "q13" => execQ13(spark)
      case "q14" => execQ14(spark)
      case "q15" => execQ15(spark)
      case "q16" => execQ16(spark)
      case "q17" => execQ17(spark)
      case "q18" => execQ18(spark)
      case "q19" => execQ19(spark)
      case "q20" => execQ20(spark)
      case "q21" => execQ21(spark)
      case "q22" => execQ22(spark)
      case "q_highbalance" => execHighBalance(spark)
      case "q_scan" => execScan(spark)
      case "q_static" => execStatic(spark)
      case "q_anti" => execAnti(spark)
      case "q_outer" => execOuter(spark)
      case "q_agg" => execAgg(spark)
      case "q_aggjoin" => execAggJoin(spark)
      case _ => printf("Not yet supported %s\n", query_name_prefix)
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

    // set aggregation interval for aggregate operations
    sum_qty.setAggregationInterval(aggregation_interval)
    sum_qty.setAggregationSchemaName("sum_qty")
    sum_base_price.setAggregationInterval(aggregation_interval)
    sum_base_price.setAggregationSchemaName("sum_base_price")
    sum_disc_price.setAggregationInterval(aggregation_interval)
    sum_disc_price.setAggregationSchemaName("sum_disc_price")
    sum_charge.setAggregationInterval(aggregation_interval)
    sum_charge.setAggregationSchemaName("sum_charge")
    avg_qty.setAggregationInterval(aggregation_interval)
    avg_qty.setAggregationSchemaName("avg_qty")
    avg_price.setAggregationInterval(aggregation_interval)
    avg_price.setAggregationSchemaName("avg_price")
    avg_disc.setAggregationInterval(aggregation_interval)
    avg_disc.setAggregationSchemaName("avg_disc")
    count_order.setAggregationInterval(aggregation_interval)
    count_order.setAggregationSchemaName("count_order")

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = l_sampled.filter($"l_shipdate" <= "1998-09-01")
      .select($"l_returnflag", $"l_linestatus", $"l_quantity", $"l_extendedprice", $"l_discount", $"l_tax")
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
      //.orderBy($"l_returnflag", $"l_linestatus")

    result.explain(extended = true, cost = true)

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ2_subquery(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val min_supplycost = new DoubleMin
    min_supplycost.setAggregationInterval(aggregation_interval)
    min_supplycost.setAggregationSchemaName("min_supplycost")

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val ps_sampled = ps.sample(withReplacement = false, fraction = SR, seed = 42)

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
    val n_sampled = n.sample(withReplacement = false, fraction = SR, seed = 42)

    val r = DataUtils.loadStreamTable(spark, "region", "r")
      .filter($"r_name" === "EUROPE")
    val r_sampled = r.sample(withReplacement = false, fraction = SR, seed = 42)

    r_sampled.join(n_sampled, $"r_regionkey" === $"n_regionkey")
      .join(s_sampled, $"n_nationkey" === $"s_nationkey")
      .join(ps_sampled, $"s_suppkey" === $"ps_suppkey")
      .groupBy($"ps_partkey")
      .agg(min_supplycost($"ps_supplycost").as("min_supplycost"))
      .select($"ps_partkey".as("min_partkey"), $"min_supplycost")
  }

  def execQ2(spark: SparkSession): Unit = {
    import spark.implicits._

    val p = DataUtils.loadStreamTable(spark, "part", "p")
      .filter(($"p_size" === 15) and ($"p_type" like("%BRASS")))
    val p_sampled = p.sample(withReplacement = false, fraction = SR, seed = 42)

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val ps_sampled = ps.sample(withReplacement = false, fraction = SR, seed = 42)

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
    val n_sampled = n.sample(withReplacement = false, fraction = SR, seed = 42)

    val r = DataUtils.loadStreamTable(spark, "region", "r")
      .filter($"r_name" === "EUROPE")
    val r_sampled = r.sample(withReplacement = false, fraction = SR, seed = 42)

    val subquery1_a = r_sampled.join(n_sampled, $"r_regionkey" === $"n_regionkey")
      .join(s_sampled, $"n_nationkey" === $"s_nationkey")

    val subquery1_b = ps_sampled.join(p_sampled, $"ps_partkey" === $"p_partkey")
    val subquery1 = subquery1_a.join(subquery1_b, $"s_suppkey" === $"ps_suppkey")

    val subquery2 = execQ2_subquery(spark)

    val result = subquery1
      .join(subquery2, ($"p_partkey" ===  $"min_partkey") and ($"ps_supplycost" === $"min_supplycost"))
      .select($"s_acctbal", $"s_name", $"n_name", $"p_partkey", $"p_mfgr", $"s_address", $"s_phone", $"s_comment")
      //.limit(100)

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ3(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Sum_disc_price
    sum_disc_price.setAggregationInterval(aggregation_interval)
    sum_disc_price.setAggregationSchemaName("revenue")

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
      .filter($"c_mktsegment" === "BUILDING")
    val c_sampled = c.sample(withReplacement = false, fraction = SR, seed = 42)

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" < "1995-03-15")
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" > "1995-03-15")
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = c.join(o_sampled, $"c_custkey" === $"o_custkey")
      .join(l_sampled, $"o_orderkey" === $"l_orderkey")
      .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
      .agg(sum_disc_price($"l_extendedprice", $"l_discount").alias("revenue"))
      //.orderBy(desc("revenue"), $"o_orderdate")
      .select("l_orderkey", "revenue", "o_orderdate", "o_shippriority")
      //.limit(10)

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ4(spark: SparkSession): Unit = {
    import spark.implicits._

    val order_count = new Count
    order_count.setAggregationInterval(aggregation_interval)
    order_count.setAggregationSchemaName("order_count")

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" >= "1993-07-01" and $"o_orderdate" < "1993-10-01")
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_commitdate" < $"l_receiptdate")
      .select("l_orderkey")
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = o_sampled.join(l_sampled, $"o_orderkey" === $"l_orderkey", "left_semi")
      .groupBy("o_orderpriority")
      .agg(order_count(lit(1)).alias("order_count"))
      //.orderBy("o_orderpriority")

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ5(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Sum_disc_price
    sum_disc_price.setAggregationInterval(aggregation_interval)
    sum_disc_price.setAggregationSchemaName("revenue")

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val c_sampled = c.sample(withReplacement = false, fraction = SR, seed = 42)

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" >= "1994-01-01" and $"o_orderdate" < "1995-01-01")
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
    val n_sampled = n.sample(withReplacement = false, fraction = SR, seed = 42)

    val r = DataUtils.loadStreamTable(spark, "region", "r")
      .filter($"r_name" === "ASIA")
    val r_sampled = r.sample(withReplacement = false, fraction = SR, seed = 42)

    val query_a = r_sampled.join(n_sampled, $"r_regionkey" === $"n_regionkey")
      .join(s_sampled, $"n_nationkey" === $"s_nationkey")

    val query_b = l_sampled.join(o_sampled, $"l_orderkey" === $"o_orderkey")
      .join(c_sampled, $"o_custkey" === $"c_custkey")

    val result =
      query_a.join(query_b, $"s_nationkey" === $"c_nationkey" and $"s_suppkey" === $"l_suppkey")
      .groupBy("n_name")
      .agg(sum_disc_price($"l_extendedprice", $"l_discount" ).alias("revenue"))
      //.orderBy(desc("revenue"))

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ6(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum
    doubleSum.setAggregationInterval(aggregation_interval)
    doubleSum.setAggregationSchemaName("revenue")

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipdate" between("1994-01-01", "1995-01-01"))
        and ($"l_discount" between(0.05, 0.07)) and ($"l_quantity" < 24))
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = l_sampled.agg(doubleSum($"l_extendedprice" * $"l_discount").alias("revenue"))

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ7(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Sum_disc_price

    sum_disc_price.setAggregationInterval(aggregation_interval)
    sum_disc_price.setAggregationSchemaName("revenue")

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1995-01-01", "1996-12-31"))
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val c_sampled = c.sample(withReplacement = false, fraction = SR, seed = 42)

    val n1 = DataUtils.loadStreamTable(spark, "nation", "n1")
      .select($"n_name".alias("supp_nation"), $"n_nationkey".as("n1_nationkey"))
    val n1_sampled = n1.sample(withReplacement = false, fraction = SR, seed = 42)

    val n2 = DataUtils.loadStreamTable(spark, "nation", "n2")
      .select($"n_name".alias("cust_nation"), $"n_nationkey".as("n2_nationkey"))
    val n2_sampled = n2.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = l_sampled.join(s_sampled, $"l_suppkey" === $"s_suppkey")
      .join(o_sampled, $"l_orderkey" === $"o_orderkey")
      .join(c_sampled, $"o_custkey" === $"c_custkey")
      .join(n1_sampled, $"s_nationkey" === $"n1_nationkey")
      .join(n2_sampled, $"c_nationkey" === $"n2_nationkey")
      .filter(($"supp_nation" === "FRANCE" and $"cust_nation" === "GERMANY")
        or ($"supp_nation" === "GERMANY" and $"cust_nation" === "FRANCE"))
      .select($"supp_nation", $"cust_nation", year($"l_shipdate").as("l_year"),
        $"l_extendedprice", $"l_discount")
      .groupBy("supp_nation", "cust_nation", "l_year")
      .agg(sum_disc_price($"l_extendedprice", $"l_discount").as("revenue"))
      //.orderBy("supp_nation", "cust_nation", "l_year")

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ8(spark: SparkSession): Unit = {
    import spark.implicits._

    val udaf_q8 = new UDAF_Q8

    udaf_q8.setAggregationInterval(aggregation_interval)
    udaf_q8.setAggregationSchemaName("mkt_share")

    val p = DataUtils.loadStreamTable(spark, "part", "p")
      .filter($"p_type" === "ECONOMY ANODIZED STEEL")
    val p_sampled = p.sample(withReplacement = false, fraction = SR, seed = 42)

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" between("1995-01-01", "1996-12-31"))
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val c_sampled = c.sample(withReplacement = false, fraction = SR, seed = 42)

    val n1 = DataUtils.loadStreamTable(spark, "nation", "n1")
      .select($"n_regionkey".alias("n1_regionkey"), $"n_nationkey".as("n1_nationkey"))
    val n1_sampled = n1.sample(withReplacement = false, fraction = SR, seed = 42)

    val n2 = DataUtils.loadStreamTable(spark, "nation", "n2")
      .select($"n_name".alias("n2_name"), $"n_nationkey".as("n2_nationkey"))
    val n2_sampled = n2.sample(withReplacement = false, fraction = SR, seed = 42)

    val r = DataUtils.loadStreamTable(spark, "region", "r")
      .filter($"r_name" === "AMERICA")
    val r_sampled = r.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = l_sampled.join(p, $"l_partkey" === $"p_partkey")
      .join(s_sampled, $"l_suppkey" === $"s_suppkey")
      .join(o_sampled, $"l_orderkey" === $"o_orderkey")
      .join(c_sampled, $"o_custkey" === $"c_custkey")
      .join(n1_sampled, $"c_nationkey" === $"n1_nationkey")
      .join(r_sampled, $"n1_regionkey" === $"r_regionkey")
      .join(n2_sampled, $"s_nationkey" === $"n2_nationkey")
      .select(year($"o_orderdate").as("o_year"),
        ($"l_extendedprice" * ($"l_discount" - 1) * -1).as("volume"), $"n2_name")
      .groupBy($"o_year")
      .agg(udaf_q8($"n2_name", $"volume").as("mkt_share"))
      //.orderBy($"o_year")

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ9(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum
    doubleSum.setAggregationInterval(aggregation_interval)
    doubleSum.setAggregationSchemaName("sum_profit")

    val p = DataUtils.loadStreamTable(spark, "part", "p")
      .filter($"p_name" like("%green%"))
    val p_sampled = p.sample(withReplacement = false, fraction = SR, seed = 42)

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val ps_sampled = ps.sample(withReplacement = false, fraction = SR, seed = 42)

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
    val n_sampled = n.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = l_sampled.join(p_sampled, $"l_partkey" === $"p_partkey")
      .join(ps_sampled, $"l_partkey" === $"ps_partkey" and $"l_suppkey" === $"ps_suppkey")
      .join(s_sampled, $"l_suppkey" === $"s_suppkey")
      .join(o_sampled, $"l_orderkey" === $"o_orderkey")
      .join(n_sampled, $"s_nationkey" === $"n_nationkey")
      .select($"n_name".as("nation"),
        year($"o_orderdate").as("o_year"),
        (($"l_extendedprice" * ($"l_discount" - 1) * -1) - $"ps_supplycost" * $"l_quantity")
          .as("amount"))
      .groupBy("nation", "o_year")
      .agg(doubleSum($"amount").as("sum_profit"))
      //.orderBy($"nation", desc("o_year"))

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ10(spark: SparkSession): Unit = {
    import spark.implicits._

    val revenue = new Sum_disc_price
    revenue.setAggregationInterval(aggregation_interval)
    revenue.setAggregationSchemaName("revenue")

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val c_sampled = c.sample(withReplacement = false, fraction = SR, seed = 42)

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" >= "1993-10-01" and $"o_orderdate" < "1994-01-01")
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_returnflag" === "R")
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
    val n_sampled = n.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = l.join(o_sampled, $"l_orderkey" === $"o_orderkey")
      .join(c_sampled, $"o_custkey" === $"c_custkey")
      .join(n_sampled, $"c_nationkey" === $"n_nationkey")
      .groupBy("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment")
      .agg(revenue($"l_extendedprice", $"l_discount").as("revenue"))
      //.orderBy(desc("revenue"))

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ11_subquery(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val doubleSum = new DoubleSum

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val ps_sampled = ps.sample(withReplacement = false, fraction = SR, seed = 42)

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
      .filter($"n_name" === "GERMANY")
    val n_sampled = n.sample(withReplacement = false, fraction = SR, seed = 42)

    s_sampled.join(n_sampled, $"s_nationkey" === $"n_nationkey")
      .join(ps_sampled, $"s_suppkey" === $"ps_suppkey")
      .agg(doubleSum($"ps_supplycost" * $"ps_availqty" * 0.0001/SF).as("small_value"))
  }

  def execQ11(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum
    doubleSum.setAggregationInterval(aggregation_interval)
    doubleSum.setAggregationSchemaName("value")

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val ps_sampled = ps.sample(withReplacement = false, fraction = SR, seed = 42)

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
      .filter($"n_name" === "GERMANY")
    val n_sampled = n.sample(withReplacement = false, fraction = SR, seed = 42)

    val subquery = execQ11_subquery(spark)

    if (iOLAPConf == iOLAP_TRAINING) {
      DataUtils.writeToSink(subquery.agg(min($"small_value"), max($"small_value")), query_name)
    }
    else {
      val result = s_sampled.join(n_sampled, $"s_nationkey" === $"n_nationkey")
          .join(ps_sampled, $"s_suppkey" === $"ps_suppkey")
          .groupBy($"ps_partkey")
          .agg(
            doubleSum($"ps_supplycost" * $"ps_availqty").as("value"))
          .join(subquery, $"value" > $"small_value", "cross")
          .select($"ps_partkey", $"value")
          //.orderBy(desc("value"))

      if (TPCHSchema.checkpointPath == "none") {
        DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
      }
      else {
        DataUtils.writeToSink(result, query_name, trigger_interval)
      }
    }
  }

  def execQ12(spark: SparkSession): Unit = {
    import spark.implicits._

    val udaf_q12_low = new UDAF_Q12_LOW
    val udaf_q12_high = new UDAF_Q12_HIGH
    udaf_q12_low.setAggregationInterval(aggregation_interval)
    udaf_q12_low.setAggregationSchemaName("low_line_count")
    udaf_q12_high.setAggregationInterval(aggregation_interval)
    udaf_q12_high.setAggregationSchemaName("high_line_count")

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipmode" === "MAIL")
        and ($"l_commitdate" < $"l_receiptdate")
        and ($"l_shipdate" < $"l_commitdate")
        and ($"l_receiptdate" === "1994-01-01"))
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = o_sampled.join(l_sampled, $"o_orderkey" === $"l_orderkey")
      .groupBy($"l_shipmode")
      .agg(
          udaf_q12_high($"o_orderpriority").as("high_line_count"),
          udaf_q12_low($"o_orderpriority").as("low_line_count")
      )
      //.orderBy($"l_shipmode")

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ13(spark: SparkSession): Unit = {
    import spark.implicits._

    val c_count = new Count_not_null
    val custdist = new Count
    c_count.setAggregationInterval(aggregation_interval)
    c_count.setAggregationSchemaName("c_count")
    custdist.setAggregationInterval(aggregation_interval)
    custdist.setAggregationSchemaName("custdist")

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val c_sampled = c.sample(withReplacement = false, fraction = SR, seed = 42)

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter(!($"o_comment" like("%special%requests%")))
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = c_sampled.join(o_sampled, $"c_custkey" === $"o_custkey", "left_outer")
      .groupBy($"c_custkey")
      .agg(c_count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(custdist(lit(1)).as("custdist"))
      //.orderBy(desc("custdist"), desc("c_count"))

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ14(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Sum_disc_price
    val udaf_q14 = new UDAF_Q14
    sum_disc_price.setAggregationInterval(aggregation_interval)
    sum_disc_price.setAggregationSchemaName("sum_disc_price")
    udaf_q14.setAggregationInterval(aggregation_interval)
    udaf_q14.setAggregationSchemaName("uadf_q14")

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1995-09-01", "1995-10-01"))
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val p = DataUtils.loadStreamTable(spark, "part", "p")
    val p_sampled = p.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = l_sampled.join(p_sampled, $"l_partkey" === $"p_partkey")
      .agg(
        ((udaf_q14($"p_type", $"l_extendedprice", $"l_discount")/
        sum_disc_price($"l_extendedprice", $"l_discount")) * 100).as("promo_revenue")
      )

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ15_subquery(spark: SparkSession): DataFrame = {
    import  spark.implicits._

    val sum_disc_price = new Sum_disc_price
    sum_disc_price.setAggregationInterval(aggregation_interval)
    sum_disc_price.setAggregationSchemaName("total_revenue")

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1996-01-01", "1996-04-01"))
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    l_sampled.groupBy($"l_suppkey")
      .agg(sum_disc_price($"l_extendedprice", $"l_discount").as("total_revenue"))
      .select($"l_suppkey".as("supplier_no"), $"total_revenue")
  }

  def execQ15(spark: SparkSession): Unit = {
    import spark.implicits._

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val revenue = execQ15_subquery(spark)
    val max_revenue = execQ15_subquery(spark).agg(max($"total_revenue").as("max_revenue"))

    val result = s_sampled.join(revenue, $"s_suppkey" === $"supplier_no")
      .join(max_revenue, $"total_revenue" >= $"max_revenue", "cross")
      .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
      //.orderBy("s_suppkey")

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ16(spark: SparkSession): Unit = {
    import spark.implicits._

    val supplier_cnt = new Count
    supplier_cnt.setAggregationInterval(aggregation_interval)
    supplier_cnt.setAggregationSchemaName("supplier_cnt")

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val ps_sampled = ps.sample(withReplacement = false, fraction = SR, seed = 42)

    val p = DataUtils.loadStreamTable(spark, "part", "part")
      .filter(($"p_brand" =!= "Brand#45") and
        (!($"p_type" like("MEDIUM POLISHED%")))
        and ($"p_size" isin(49, 14, 23, 45, 19, 3, 36, 9)))
    val p_sampled = p.sample(withReplacement = false, fraction = SR, seed = 42)

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
      .filter($"s_comment" like("%Customer%Complaints%"))
      .select($"s_suppkey")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = ps_sampled.join(p_sampled, $"ps_partkey" === $"p_partkey")
      .join(s_sampled, $"ps_suppkey" === $"s_suppkey", "left_anti")
      .select($"p_brand", $"p_type", $"p_size", $"ps_suppkey")
      //.dropDuplicates()
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(supplier_cnt($"ps_suppkey").as("supplier_cnt"))
      //.orderBy(desc("supplier_cnt"), $"p_brand", $"p_type", $"p_size")

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ17(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val doubleSum = new DoubleSum
    doubleAvg.setAggregationInterval(aggregation_interval)
    doubleAvg.setAggregationSchemaName("avg_quantity")
    doubleSum.setAggregationInterval(aggregation_interval)
    doubleSum.setAggregationSchemaName("avg_yearly")

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val p = DataUtils.loadStreamTable(spark, "part", "p")
      .filter($"p_brand" === "Brand#23" and $"p_container" === "MED BOX")
    val p_sampled = p.sample(withReplacement = false, fraction = SR, seed = 42)

    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .groupBy($"l_partkey")
      .agg((doubleAvg($"l_quantity") * 0.2).as("avg_quantity"))
      .select($"l_partkey".as("agg_l_partkey"), $"avg_quantity")
    val agg_l_sampled = agg_l.sample(withReplacement = false, fraction = SR, seed = 42)

    if (iOLAPConf == iOLAP_TRAINING) {
      val tmpDF = l.join(agg_l, $"l_partkey" === $"agg_l_partkey"
        and $"l_quantity" < $"avg_quantity").select($"l_partkey")
        .dropDuplicates()

      val fullP = DataUtils.loadStreamTable(spark, "part", "p")
      val result = fullP.join(tmpDF, $"p_partkey" === $"l_partkey", "left_anti")
          .select($"p_partkey")

      DataUtils.writeToFile(result, query_name, hdfsRoot + iOLAP_Q17_dst)

    }
    else {
      val result =
        l_sampled.join(agg_l_sampled, $"l_partkey" === $"agg_l_partkey" and $"l_quantity" < $"avg_quantity")
          .join(p_sampled, $"l_partkey" === $"p_partkey")
          .agg((doubleSum($"l_extendedprice") / 7.0).as("avg_yearly"))

      if (TPCHSchema.checkpointPath == "none") {
        DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
      }
      else {
        DataUtils.writeToSink(result, query_name, trigger_interval)
      }
    }
  }

  def execQ18(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleSum1 = new DoubleSum
    val doubleSum2 = new DoubleSum
    doubleSum1.setAggregationInterval(aggregation_interval)
    doubleSum1.setAggregationSchemaName("avg_quantity")
    doubleSum2.setAggregationInterval(aggregation_interval)
    doubleSum2.setAggregationSchemaName("avg_yearly")

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    val c_sampled = c.sample(withReplacement = false, fraction = SR, seed = 42)

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .groupBy("l_orderkey")
      .agg(doubleSum1($"l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("agg_orderkey"))
    val agg_l_sampled = agg_l.sample(withReplacement = false, fraction = SR, seed = 42)

    if (iOLAPConf == iOLAP_TRAINING) {
       DataUtils.writeToFile(agg_l, query_name, hdfsRoot + iOLAP_Q18_dst)
    }
    else {
      val result =
        o.join(agg_l_sampled, $"o_orderkey" === $"agg_orderkey", "left_semi")
            .join(l_sampled, $"o_orderkey" === $"l_orderkey")
            .join(c_sampled, $"o_custkey" === $"c_custkey")
            .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
            .agg(doubleSum2($"l_quantity"))

      if (TPCHSchema.checkpointPath == "none") {
        DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
      }
      else {
        DataUtils.writeToSink(result, query_name, trigger_interval)
      }
    }
  }

  def execQ19(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Sum_disc_price
    sum_disc_price.setAggregationInterval(aggregation_interval)
    sum_disc_price.setAggregationSchemaName("revenue")

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipmode" isin("AIR", "AIR REG"))
        and ($"l_shipinstruct" === "DELIVER IN PERSON"))
    val l_sampled = l.sample(withReplacement = false, fraction = SR, seed = 42)

    val p = DataUtils.loadStreamTable(spark, "part", "p")
    val p_sampled = p.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = l_sampled.join(p_sampled, $"l_partkey" === $"p_partkey"
      and ((($"p_brand" === "Brand#12") and
      ($"p_container" isin("SM CASE", "SM BOX", "SM PACK", "SM PKG")) and
      ($"l_quantity" >= 1 and $"l_quantity" <= 11) and
      ($"p_size" between(1, 5)))
      or (($"p_brand" === "Brand#23") and
      ($"p_container" isin("MED BAG", "MED BOX", "MED PKG", "MED PACK")) and
      ($"l_quantity" >= 10 and $"l_quantity" <= 20) and
      ($"p_size" between(1, 10)))
      or (($"p_brand" === "Brand#34") and
      ($"p_container" isin("LG CASE", "LG BOX", "LG PACK", "LG PKG")) and
      ($"l_quantity" >= 20 and $"l_quantity" <= 30) and
      ($"p_size" between(1, 15)))))
      .agg(sum_disc_price($"l_extendedprice", $"l_discount").as("revenue"))

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ20(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleSum = new DoubleSum
    doubleSum.setAggregationInterval(aggregation_interval)
    doubleSum.setAggregationSchemaName("agg_l_sum")

    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1994-01-01", "1994-12-31"))
      .groupBy($"l_partkey", $"l_suppkey")
      .agg((doubleSum($"l_quantity") * 0.5).as("agg_l_sum"))
      .select($"l_partkey".as("agg_l_partkey"),
      $"l_suppkey".as("agg_l_suppkey"),
      $"agg_l_sum")
    val agg_l_sampled = agg_l.sample(withReplacement = false, fraction = SR, seed = 42)

    val p = DataUtils.loadStreamTable(spark, "part", "p")
      .filter($"p_name" like("forest%"))
    val p_sampled = p.sample(withReplacement = false, fraction = SR, seed = 42)

    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val ps_sampled = ps.sample(withReplacement = false, fraction = SR, seed = 42)

    val subquery = ps_sampled.join(agg_l_sampled, $"ps_partkey" === $"agg_l_partkey"
        and $"ps_suppkey" === $"agg_l_suppkey" and $"ps_availqty" > $"agg_l_sum")
      .join(p_sampled, $"ps_partkey" === $"p_partkey", "left_semi")
      .select("ps_suppkey")

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
      .filter($"n_name" === "CANADA")
    val n_sampled = n.sample(withReplacement = false, fraction = SR, seed = 42)

    if (iOLAPConf == iOLAP_TRAINING) {
      DataUtils.writeToFile(subquery, query_name, hdfsRoot + iOLAP_Q20_dst)
    }
    else {
      val result =
        s_sampled.join(subquery, $"s_suppkey" === $"ps_suppkey", "left_semi")
          .join(n_sampled, $"s_nationkey" === $"n_nationkey")
          .select($"s_name", $"s_address")

      if (TPCHSchema.checkpointPath == "none") {
        DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
      }
      else {
        DataUtils.writeToSink(result, query_name, trigger_interval)
      }
    }
  }

  def execQ21(spark: SparkSession): Unit = {
    import spark.implicits._

    val count = new Count
    count.setAggregationInterval(aggregation_interval)
    count.setAggregationSchemaName("numwait")

    val s = DataUtils.loadStreamTable(spark, "supplier", "s")
    val s_sampled = s.sample(withReplacement = false, fraction = SR, seed = 42)

    val l1 = DataUtils.loadStreamTable(spark, "lineitem", "l1")
      .filter($"l_receiptdate" > $"l_commitdate")
    val l1_sampled = l1.sample(withReplacement = false, fraction = SR, seed = 42)

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderstatus" === "F")
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    val n = DataUtils.loadStreamTable(spark, "nation", "n")
      .filter($"n_name" === "SAUDI ARABIA")
    val n_sampled = n.sample(withReplacement = false, fraction = SR, seed = 42)

    val init_result = l1_sampled.join(o_sampled, $"l_orderkey" === $"o_orderkey")
      .join(s_sampled, $"l_suppkey" === $"s_suppkey")
      .join(n_sampled, $"s_nationkey" === $"n_nationkey")

    val l2 = DataUtils.loadStreamTable(spark, "lineitem", "l2")
      .select($"l_orderkey".as("l2_orderkey"), $"l_suppkey".as("l2_suppkey"))
    val l2_sampled = l2.sample(withReplacement = false, fraction = SR, seed = 42)

    val l3 = DataUtils.loadStreamTable(spark, "lineitem", "l3")
      .filter($"l_receiptdate" > $"l_commitdate")
      .select($"l_orderkey".as("l3_orderkey"), $"l_suppkey".as("l3_suppkey"))
    val l3_sampled = l3.sample(withReplacement = false, fraction = SR, seed = 42)

    val result = init_result
      .join(l2_sampled, ($"l_orderkey" === $"l2_orderkey")
        and ($"l_suppkey" =!= $"l2_suppkey"), "left_semi")
      .join(l3_sampled, ($"l_orderkey" === $"l3_orderkey")
        and ($"l_suppkey" =!= $"l3_suppkey"), "left_anti")
      .groupBy("s_name")
      .agg(count(lit(1)).as("numwait"))
      //.orderBy(desc("numwait"), $"s_name")

    if (TPCHSchema.checkpointPath == "none") {
      DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
    }
    else {
      DataUtils.writeToSink(result, query_name, trigger_interval)
    }
  }

  def execQ22(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val numcust = new Count
    val doubleSum = new DoubleSum
    doubleAvg.setAggregationInterval(aggregation_interval)
    doubleAvg.setAggregationSchemaName("avg_acctbal")
    numcust.setAggregationInterval(aggregation_interval)
    numcust.setAggregationSchemaName("numcust")
    doubleSum.setAggregationInterval(aggregation_interval)
    doubleSum.setAggregationSchemaName("totalacctbal")

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
      .filter(substring($"c_phone", 1, 2)
        isin("13", "31", "23", "29", "30", "18", "17"))
    val c_sampled = c.sample(withReplacement = false, fraction = SR, seed = 42)

    val subquery1 = DataUtils.loadStreamTable(spark, "customer", "c1")
      .filter((substring($"c_phone", 1, 2)
        isin("13", "31", "23", "29", "30", "18", "17")) and
        ($"c_acctbal" > 0.00))
      .agg(doubleAvg($"c_acctbal").as("avg_acctbal"))
    val subquery1_sampled = subquery1.sample(withReplacement = false, fraction = SR, seed = 42)

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
    val o_sampled = o.sample(withReplacement = false, fraction = SR, seed = 42)

    if (iOLAPConf == iOLAP_TRAINING) {
      DataUtils.writeToSink(subquery1.agg(min($"avg_acctbal"), max($"avg_acctbal")), query_name)
    }
    else {
      val result =
        c_sampled.join(o_sampled, $"c_custkey" === $"o_custkey", "left_anti")
          .join(subquery1_sampled, $"c_acctbal" > $"avg_acctbal", "cross")
          .select(substring($"c_phone", 1, 2).as("cntrycode"), $"c_acctbal")
          .groupBy($"cntrycode")
          .agg(numcust(lit(1)).as("numcust"), doubleSum($"c_acctbal").as("totalacctbal"))
          //.orderBy($"cntrycode")

      if (TPCHSchema.checkpointPath == "none") {
        DataUtils.writeToSinkNoCKPT(result, query_name, trigger_interval)
      }
      else {
        DataUtils.writeToSink(result, query_name, trigger_interval)
      }
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

  def execAnti(spark: SparkSession): Unit = {
    import spark.implicits._

    val count = new Count

    val p = DataUtils.loadStreamTable(spark, "part", "p")
    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")
    val result = p
        .join(ps, $"p_partkey" === $"ps_partkey", "left_anti")
        .join(l, $"p_partkey" === $"l_partkey")
        .join(o, $"l_orderkey" === $"o_orderkey")
        .agg(count(lit(1L)))

    DataUtils.writeToSink(result, query_name)
  }

  def execOuter(spark: SparkSession): Unit = {
    import spark.implicits._

    val count = new Count

    val p = DataUtils.loadStreamTable(spark, "part", "p")
    val ps = DataUtils.loadStreamTable(spark, "partsupp", "ps")
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    val o = DataUtils.loadStreamTable(spark, "orders", "o")

    val result = p
        .join(ps, $"p_partkey" === $"ps_partkey", "left_outer")
        .filter($"ps_partkey".isNull || ($"ps_partkey".isNotNull && $"ps_partkey"%10000 === 0))
        .join(l, $"p_partkey" === $"l_partkey")
        .join(o, $"l_orderkey" === $"o_orderkey")
        .agg(count(lit(1L)))

    DataUtils.writeToSink(result, query_name)
  }

  def execAgg(spark: SparkSession): Unit = {
    import spark.implicits._

    val c_count = new Count_not_null
    val custdist = new Count

    val o = DataUtils.loadStreamTable(spark, "orders", "o")

    val result = o
      .groupBy($"o_custkey")
      .agg(
        c_count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(
        custdist(lit(1)).as("custdist"))

    DataUtils.writeToSink(result, query_name)
  }

  def execAggJoin(spark: SparkSession): Unit = {
    import spark.implicits._

    val doubleAvg = new DoubleAvg
    val doubleAvg2 = new DoubleAvg

    val c = DataUtils.loadStreamTable(spark, "customer", "c")
    // val o = DataUtils.loadStreamTable(spark, "orders", "o")

    val agg_o = DataUtils.loadStreamTable(spark, "orders", "o")
      .groupBy("o_custkey")
      .agg(doubleAvg($"o_totalprice").as("avg_totalprice"))
      .select($"o_custkey", $"avg_totalprice")

    val result = agg_o
          .join(c, $"o_custkey" === $"c_custkey")
          .agg(doubleAvg2($"avg_totalprice").as("sum_totalprice"))

    DataUtils.writeToSink(result, query_name)
  }
}

object QueryTPCH {
  def main(args: Array[String]): Unit = {

    if (args.length < 21) {
      System.err.println("Usage: QueryTPCH" +
        "<bootstrap-servers> <query> <numBatch> <number-shuffle-partition> <statistics dir>" +
        "<statistics dir> <SF> <HDFS root> <execution mode> <num of input partitions> <performance constraint>" +
        "<large dataset> <iOLAP Config> <inc_pct> <cost model bias> <max step> <sample time>" +
        "<sample rate> <trigger interval> <aggregation interval> <checkpoint>")
      System.exit(1)
    }

    val tpch = new QueryTPCH(args(0), args(1), args(2).toInt, args(3),
      args(4), args(5), args(6).toDouble, args(7), args(8), args(9).toInt,
      args(10), args(11).toBoolean, args(12).toInt, args(13), args(14), args(15),
      args(16), args(17).toDouble, args(18).toInt, args(19).toInt, args(20), args(21))
    tpch.execQuery()
  }
}

// scalastyle:off println
