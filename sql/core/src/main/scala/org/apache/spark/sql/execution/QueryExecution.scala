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

package org.apache.spark.sql.execution

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, XXXXBroadcastDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.aggregate.{XXXXFinalAggExec, XXXXHashAggregateExec}
import org.apache.spark.sql.execution.command.{DescribeTableCommand, ExecutedCommandExec, ShowTablesCommand}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.streaming.{MicroBatchExecution, XXXXSimpleHashJoinExec, XXXXSymmetricHashJoinExec, XXXXThetaJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DateType, DecimalType, TimestampType, _}
import org.apache.spark.util.Utils

/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryExecution(val sparkSession: SparkSession, val logical: LogicalPlan) {

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = analyzed

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  lazy val analyzed: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.analyzer.executeAndCheck(logical)
  }

  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    sparkSession.sharedState.cacheManager.useCachedData(analyzed)
  }

  lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)

  lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(optimizedPlan)).next()
  }

  private def optimizeProjJoinPattern(plan: SparkPlan): Unit = {
    plan match {
      case projPlan: ProjectExec if projPlan.children.nonEmpty =>
        if (projPlan.child.isInstanceOf[XXXXSymmetricHashJoinExec]) {
          val joinPlan = projPlan.child.asInstanceOf[XXXXSymmetricHashJoinExec]
          joinPlan.setPropagateUpdate(projPlan.output)
        } else if (projPlan.child.isInstanceOf[XXXXThetaJoinExec]) {
          val joinPlan = projPlan.child.asInstanceOf[XXXXThetaJoinExec]
          joinPlan.setPropagateUpdate(projPlan.output)
        }
      case _ =>
    }
    plan.children.foreach(child => optimizeProjJoinPattern(child))
  }

  private def markDeltaOutput(plan: SparkPlan): Unit = {
    plan match {
      case _: XXXXSymmetricHashJoinExec | _: XXXXThetaJoinExec =>
      case aggPlan: XXXXHashAggregateExec =>
        aggPlan.setDeltaOutput(false)
      case _ =>
        plan.children.foreach(child => markDeltaOutput(child))
    }
  }

  private def findValidNumPartition(plan: SparkPlan): Int = {
    if (plan == null) {
      throw new IllegalArgumentException("Not found valid number of partitions")
    }

    plan match {
      case scan: DataSourceV2ScanExec =>
        return scan.partitions.size
      case localScan: LocalTableScanExec =>
        return 1
      case _ =>
    }

    if (plan.outputPartitioning.numPartitions != 0) {
      return plan.outputPartitioning.numPartitions
    }

    return plan.children.map(child => findValidNumPartition(child)).max
  }

  private def updatePartitioningforThetaJoin(plan: SparkPlan): Unit = {
    plan match {
      case thetaJoin: XXXXThetaJoinExec =>
        require(thetaJoin.right.isInstanceOf[ShuffleExchangeExec],
          "Right child of ThetaJoin needs to be ShuffleExechangeExec")
        val left = thetaJoin.left
        val right = thetaJoin.right.asInstanceOf[ShuffleExchangeExec]
        val XXXXBroadCast = new XXXXBroadcastDistribution()
        val XXXXNumPartitions = findValidNumPartition(left)
        right.withNewPartitioning(XXXXBroadCast.createPartitioning(XXXXNumPartitions))
      case _ =>
    }
    plan.children.foreach(child => updatePartitioningforThetaJoin(child))
  }

  private def setUpdateOutput(plan: SparkPlan, updateAttrs: Seq[Attribute]): Unit = {
    if (plan == null || plan.isInstanceOf[ShuffleExchangeExec]) return
    if (!XXXXUtils.attrIntersect(plan.output, updateAttrs).isEmpty) {
      plan.setUpdateOutput(false)
      plan.children.foreach(child => setUpdateOutput(child, updateAttrs))
    }
  }

  private def getUpdateAttributes(plan: SparkPlan) : Seq[Attribute] = {
    if (plan == null) return Seq()

    plan match {
      case thetaJoinExec: XXXXThetaJoinExec =>
        XXXXUtils.attrUnion(getUpdateAttributes(thetaJoinExec.left),
          getUpdateAttributes(thetaJoinExec.right))

      case equalJoinExec: XXXXSymmetricHashJoinExec =>
        XXXXUtils.attrUnion(getUpdateAttributes(equalJoinExec.left),
          getUpdateAttributes(equalJoinExec.right))

      case aggExec: XXXXFinalAggExec =>
        aggExec.findUpdateAttributes()

      case shuffleExec: ShuffleExchangeExec
        if shuffleExec.newPartitioning.isInstanceOf[HashPartitioning] =>
        val partAttrs = shuffleExec.newPartitioning.asInstanceOf[HashPartitioning]
          .expressions.map(_.asInstanceOf[NamedExpression].toAttribute)
        val retUpdateAttrs = getUpdateAttributes(plan.children(0))
        setUpdateOutput(shuffleExec.children(0),
          XXXXUtils.attrIntersect(partAttrs, retUpdateAttrs))
        retUpdateAttrs

      case projExec: ProjectExec =>
        XXXXUtils.attrIntersect(getUpdateAttributes(plan.children(0)), projExec.output)

      case _ => // FilterExec, DataSourceV2Scan, HashAggFinal, Sort
        if (plan.children != null && !plan.children.isEmpty) {
          getUpdateAttributes(plan.children(0))
        } else Seq()
    }
  }

  private def projEqual(projA: XXXXProjectExec, projB: XXXXProjectExec): Boolean = {
    val childA = projA.child.output
    val childB = projB.child.output
    val outputA = projA.projectList
    val outputB = projB.projectList

    return (!childA.zip(childB).exists(pair => !pair._1.semanticEquals(pair._2)) &&
      !outputA.zip(outputB).exists(pair => !pair._1.semanticEquals(pair._2)))
  }

  private def findProj(proj: XXXXProjectExec, microExec: MicroBatchExecution): Long = {
    val pair = microExec.projArray.find(pair => projEqual(pair._1, proj))
    if (pair.isDefined) pair.get._2
    else -1
  }

  private def assignProjId(runId: UUID, proj: XXXXProjectExec,
                           microExec: MicroBatchExecution): Unit = {
    val curProjId = findProj(proj, microExec)
    // New one, first run
    if (curProjId == -1 && microExec.currentBatchId < 1) {
      proj.setID(microExec.projId, runId)
      microExec.projArray.append(new Tuple2(proj, microExec.projId))
      microExec.projId = microExec.projId + 1
    } else {
      if (curProjId == -1) proj.setID(-1, runId)
      else proj.setID(curProjId, runId)
    }
  }

  private def setAllProjId(runId: UUID, plan: SparkPlan, microExec: MicroBatchExecution): Unit = {
    if (plan == null) return
    if (plan.isInstanceOf[XXXXProjectExec]) {
      assignProjId(runId, plan.asInstanceOf[XXXXProjectExec], microExec)
    }
    plan.children.foreach(plan => setAllProjId(runId, plan, microExec))
  }

  private def filterEqual(filterA: XXXXFilterExec, filterB: XXXXFilterExec): Boolean = {
    val childA = filterA.child.output
    val childB = filterB.child.output
    val conditionA = filterA.condition
    val conditionB = filterB.condition

    return (!childA.zip(childB).exists(pair => !pair._1.semanticEquals(pair._2)) &&
      conditionA.semanticEquals(conditionB))
  }

  private def findFilter(filter: XXXXFilterExec, microExec: MicroBatchExecution): Long = {
    val pair = microExec.filterArray.find(pair => filterEqual(pair._1, filter))
    if (pair.isDefined) pair.get._2
    else -1
  }

  private def assignFilterId(runId: UUID, filter: XXXXFilterExec,
                           microExec: MicroBatchExecution): Unit = {
    val curFilterId = findFilter(filter, microExec)
    // New one, first run
    if (curFilterId == -1 && microExec.currentBatchId < 1) {
      filter.setID(microExec.filterId, runId)
      microExec.filterArray.append(new Tuple2(filter, microExec.filterId))
      microExec.filterId = microExec.filterId + 1
    } else {
      if (curFilterId == -1) filter.setID(-1, runId)
      else filter.setID(curFilterId, runId)
    }
  }

  private def setAllFilterId(runId: UUID, plan: SparkPlan, microExec: MicroBatchExecution): Unit = {
    if (plan == null) return
    if (plan.isInstanceOf[XXXXFilterExec]) {
      assignFilterId(runId, plan.asInstanceOf[XXXXFilterExec], microExec)
    }
    plan.children.foreach(plan => setAllFilterId(runId, plan, microExec))
  }

  private def findAgg(plan: SparkPlan): XXXXHashAggregateExec = {
    plan match {
      case aggPlan: XXXXHashAggregateExec =>
        return aggPlan
      case _ =>
        findAgg(plan.children(0))
    }
  }

  private def setFinalAggId(plan: SparkPlan, finalAggStartId: Long): Unit = {
    if (plan == null) return
    plan match {
      case XXXXFinalAggExec: XXXXFinalAggExec =>
        val stateInfo = findAgg(XXXXFinalAggExec).stateInfo.get
        XXXXFinalAggExec.setId(stateInfo.operatorId + finalAggStartId,
          stateInfo.queryRunId)
      case _ =>
    }
    plan.children.foreach(setFinalAggId(_, finalAggStartId))
  }

  private def initialStarupTime = 3000
  private def perExecutionStartupTime = 60
  private def joinStartupTime = 15
  private def aggStartupTime = 15
  private def filterStartupTime = 5
  private def sourceStartupTime = 5

  private def getPerOpStartUpTime(plan: SparkPlan): Long = {
    val childCost = plan.children.map(getPerOpStartUpTime).sum

    plan match {
      case _: XXXXHashAggregateExec =>
        childCost + aggStartupTime
      case _: XXXXSymmetricHashJoinExec =>
        childCost + joinStartupTime
      case _: XXXXThetaJoinExec =>
        childCost + joinStartupTime
      case _: XXXXFilterExec =>
        childCost + filterStartupTime
      case _: DataSourceV2ScanExec =>
        childCost + sourceStartupTime
      case _ =>
        childCost
    }
  }

  private def setStartUpTime(plan: SparkPlan, isFirstBatch: Boolean): Unit = {
    plan match {
      case sink: WriteToDataSourceV2Exec =>
        val startUpTime =
          if (isFirstBatch) initialStarupTime + perExecutionStartupTime
          else perExecutionStartupTime
       sink.setStartUpTime(getPerOpStartUpTime(sink) + startUpTime)
      case _ =>
    }
  }

  private def setFirstBatch(plan: SparkPlan, isFirstBatch: Boolean): Unit = {
    if (plan == null) return
    plan match {
      case simpleJoin: XXXXSimpleHashJoinExec =>
        simpleJoin.setIsFirstBatch(isFirstBatch)
      case _ =>
    }
    plan.children.foreach(child => setFirstBatch(child, isFirstBatch))
  }

  def setRepairMode(plan: SparkPlan, repairMode: Boolean): Unit = {
    if (plan == null) return
    plan match {
      case XXXXHash: XXXXHashAggregateExec =>
        XXXXHash.setRepairMode(repairMode)
      case _ =>
    }
    plan.children.foreach(child => setRepairMode(child, repairMode))
  }

  // Several optimization techniques by XXXXDB
  def XXXXdbOptimization(runId: UUID, microExec: MicroBatchExecution): Unit = {
    // XXXXDB: Set delta output for the last aggregate
    // TODO: this assumes sort operator, if exists,
    // TODO: is at the end of a query plan preceded by an aggregate
    // TODO: and the sort operator always recomputes
    markDeltaOutput(executedPlan)

    // XXXXDB: Set whether we need to propogate updates from join operators
    // This is based on the observation where if the projected output columns
    // do not have overlap with the non-key columns from child operators,
    // we do not need to propagate the updates
    optimizeProjJoinPattern(executedPlan)

    // XXXXDB
    updatePartitioningforThetaJoin(executedPlan)

    // Output Update
    getUpdateAttributes(executedPlan)

    // Set Proj Id
    setAllProjId(runId, executedPlan, microExec)

    // Set Filter Id
    setAllFilterId(runId, executedPlan, microExec)

    // Set FinalAgg Id
    setFinalAggId(executedPlan, microExec.finalAggStartId)

    // Set startup time
    setStartUpTime(executedPlan, microExec.currentBatchId == 0)

    // Set isFirstBatch for Static Tables
    setFirstBatch(executedPlan, microExec.currentBatchId == 0)

    // Set repair mode
    // val repairConf = sparkSession.conf.get(SQLConf.XXXXDB_ENABLE_INCREMENTABILITY)
    // if (repairConf.isDefined && repairConf.get) {
    //   setRepairMode(executedPlan, microExec.currentBatchId == 0 || microExec.isLastBatch)
    // } else {
    //   setRepairMode(executedPlan, true)
    // }
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

  /** Internal version of the RDD. Avoids copies and has no schema */
  lazy val toRdd: RDD[InternalRow] = executedPlan.execute()

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  /** A sequence of rules that will be applied in order to the physical plan before execution. */
  protected def preparations: Seq[Rule[SparkPlan]] = Seq(
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf))

  protected def stringOrError[A](f: => A): String =
    try f.toString catch { case e: AnalysisException => e.toString }


  /**
   * Returns the result as a hive compatible sequence of strings. This is used in tests and
   * `SparkSQLDriver` for CLI applications.
   */
  def hiveResultString(): Seq[String] = executedPlan match {
    case ExecutedCommandExec(desc: DescribeTableCommand) =>
      // If it is a describe command for a Hive table, we want to have the output format
      // be similar with Hive.
      desc.run(sparkSession).map {
        case Row(name: String, dataType: String, comment) =>
          Seq(name, dataType,
            Option(comment.asInstanceOf[String]).getOrElse(""))
            .map(s => String.format(s"%-20s", s))
            .mkString("\t")
      }
    // SHOW TABLES in Hive only output table names, while ours output database, table name, isTemp.
    case command @ ExecutedCommandExec(s: ShowTablesCommand) if !s.isExtended =>
      command.executeCollect().map(_.getString(1))
    case other =>
      val result: Seq[Seq[Any]] = other.executeCollectPublic().map(_.toSeq).toSeq
      // We need the types so we can output struct field names
      val types = analyzed.output.map(_.dataType)
      // Reformat to match hive tab delimited output.
      result.map(_.zip(types).map(toHiveString)).map(_.mkString("\t"))
  }

  /** Formats a datum (based on the given data type) and returns the string representation. */
  private def toHiveString(a: (Any, DataType)): String = {
    val primitiveTypes = Seq(StringType, IntegerType, LongType, DoubleType, FloatType,
      BooleanType, ByteType, ShortType, DateType, TimestampType, BinaryType)

    def formatDecimal(d: java.math.BigDecimal): String = {
      if (d.compareTo(java.math.BigDecimal.ZERO) == 0) {
        java.math.BigDecimal.ZERO.toPlainString
      } else {
        d.stripTrailingZeros().toPlainString
      }
    }

    /** Hive outputs fields of structs slightly differently than top level attributes. */
    def toHiveStructString(a: (Any, DataType)): String = a match {
      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString((v, t.dataType))}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_, _], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "null"
      case (s: String, StringType) => "\"" + s + "\""
      case (decimal, DecimalType()) => decimal.toString
      case (interval, CalendarIntervalType) => interval.toString
      case (other, tpe) if primitiveTypes contains tpe => other.toString
    }

    a match {
      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString((v, t.dataType))}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_, _], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "NULL"
      case (d: Date, DateType) =>
        DateTimeUtils.dateToString(DateTimeUtils.fromJavaDate(d))
      case (t: Timestamp, TimestampType) =>
        DateTimeUtils.timestampToString(DateTimeUtils.fromJavaTimestamp(t),
          DateTimeUtils.getTimeZone(sparkSession.sessionState.conf.sessionLocalTimeZone))
      case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)
      case (decimal: java.math.BigDecimal, DecimalType()) => formatDecimal(decimal)
      case (interval, CalendarIntervalType) => interval.toString
      case (other, tpe) if primitiveTypes.contains(tpe) => other.toString
    }
  }

  def simpleString: String = withRedaction {
    s"""== Physical Plan ==
       |${stringOrError(executedPlan.treeString(verbose = false))}
      """.stripMargin.trim
  }

  override def toString: String = withRedaction {
    def output = Utils.truncatedString(
      analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}"), ", ")
    val analyzedPlan = Seq(
      stringOrError(output),
      stringOrError(analyzed.treeString(verbose = true))
    ).filter(_.nonEmpty).mkString("\n")

    s"""== Parsed Logical Plan ==
       |${stringOrError(logical.treeString(verbose = true))}
       |== Analyzed Logical Plan ==
       |$analyzedPlan
       |== Optimized Logical Plan ==
       |${stringOrError(optimizedPlan.treeString(verbose = true))}
       |== Physical Plan ==
       |${stringOrError(executedPlan.treeString(verbose = true))}
    """.stripMargin.trim
  }

  def stringWithStats: String = withRedaction {
    // trigger to compute stats for logical plans
    optimizedPlan.stats

    // only show optimized logical plan and physical plan
    s"""== Optimized Logical Plan ==
        |${stringOrError(optimizedPlan.treeString(verbose = true, addSuffix = true))}
        |== Physical Plan ==
        |${stringOrError(executedPlan.treeString(verbose = true))}
    """.stripMargin.trim
  }

  /**
   * Redact the sensitive information in the given string.
   */
  private def withRedaction(message: String): String = {
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, message)
  }

  /** A special namespace for commands that can be used to debug query execution. */
  // scalastyle:off
  object debug {
  // scalastyle:on

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def codegen(): Unit = {
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }

    /**
     * Get WholeStageCodegenExec subtrees and the codegen in a query plan
     *
     * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
     */
    def codegenToSeq(): Seq[(String, String)] = {
      org.apache.spark.sql.execution.debug.codegenStringSeq(executedPlan)
    }
  }
}
