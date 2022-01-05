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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

class LoadTPCH (bootstrap: String,
                data_root_dir: String,
                checkpoint: String,
                largeDataset: Boolean) {
  TPCHSchema.datadir = data_root_dir
  TPCHSchema.checkpointLocation = checkpoint
  TPCHSchema.largeDataset = largeDataset

  def loadOneTable(tableName: String,
                   schema: StructType,
                   path: String,
                   topics: String): Unit = {
    printf(s"Loading into ${topics} with path ${path}\n")
    val spark = SparkSession.builder()
      .appName("Loading " + tableName)
      .getOrCreate()

    val rows = spark.readStream
      .format("csv")
      .option("sep", "|")
      .schema(schema)
      .load(path)

    val query = rows.select(to_avro(struct("*")) as "value")
      .writeStream
      .format("kafka")
      .option("topic", topics)
      .option("kafka.bootstrap.servers", bootstrap)
      .option("checkpointLocation", TPCHSchema.checkpointLocation + "/" + tableName.toLowerCase)
      .start()

    query.awaitTermination()
  }

  def loadTable(tableName: String): Unit = {
    TPCHSchema.GetMetaData(tableName) match {
      case Some((schema, _, path, _, topics, _, _)) if schema != null && topics != null =>
        loadOneTable(tableName, schema, path, topics)
      case _ =>
        return
    }
  }
}

class WritingThread(streamLoader: LoadTPCH, tableName: String) extends Thread {
  override def run(): Unit = {
    streamLoader.loadTable(tableName)
  }
}

object LoadTPCH {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: LoadTPCH <bootstrap-servers> <data-root-dir> <checkpoint> <largeDataset>")
      System.exit(1)
    }

    val largeDataset = args(3).toBoolean
    val loader = new LoadTPCH(args(0), args(1), args(2), largeDataset)

    val loadTables = List("Part", "PartSupp", "Supplier", "Customer", "Orders", "Lineitem", "Nation", "Region")
    val loadThreads = loadTables.map(new WritingThread(loader, _))

    loadThreads.map(_.start())
    loadThreads.map(_.join())
  }
}
// scalastyle:off println
