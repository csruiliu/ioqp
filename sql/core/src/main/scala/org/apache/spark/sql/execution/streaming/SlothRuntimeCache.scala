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


package org.apache.spark.sql.execution.streaming

import java.util.UUID

import scala.collection.mutable

abstract class XXXXRuntime {}

// case class XXXXRuntimeOpId(var operatorId: Long, var queryRunId: UUID)
class XXXXRuntimeOpId(var operatorId: Long, var queryRunId: UUID)

object XXXXRuntimeCache {

  private val cachedXXXXRuntime =
    new mutable.HashMap[XXXXRuntimeOpId, mutable.ArrayBuffer[XXXXRuntime]]()

  def get(rtId: XXXXRuntimeOpId): XXXXRuntime = {
    cachedXXXXRuntime.synchronized {
      val rtBuf = cachedXXXXRuntime.get(rtId)
      if (!rtBuf.isDefined || rtBuf.get.isEmpty) {
        if (!rtBuf.isDefined) {
          val curQueryId = rtId.queryRunId
          cachedXXXXRuntime.toSeq
            .foreach {pair => {
              val tmpRtId = pair._1
              if (!curQueryId.equals(tmpRtId.queryRunId)) {
                cachedXXXXRuntime.remove(tmpRtId)
              }
            }}
        }
        null
      } else {
        val realRtBuf = rtBuf.get
        realRtBuf.remove(0)
      }
    }
  }

  def put(rtId: XXXXRuntimeOpId, XXXXRuntime: XXXXRuntime): Unit = {
    cachedXXXXRuntime.synchronized {
      if (!cachedXXXXRuntime.get(rtId).isDefined) {
        val buf = new mutable.ArrayBuffer[XXXXRuntime]()
        cachedXXXXRuntime.put(rtId, buf)
      }
      cachedXXXXRuntime.get(rtId).get.append(XXXXRuntime)
    }
  }
}
