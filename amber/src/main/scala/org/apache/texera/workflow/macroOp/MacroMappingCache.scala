/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.workflow.macroOp

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.workflow.macroOp.MacroExpander.MacroProvenance

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentHashMap
import scala.util.Try

/**
  * Process-singleton cache for the macro-instance provenance map produced by
  * `MacroExpander.takeMacroInstanceMapping()` after each compile. Keyed by
  * (workflowId, executionId) so multiple concurrent executions don't collide.
  *
  * Lifecycle: written by `WorkflowCompiler.compile` immediately after macro
  * expansion. Read by the REST endpoint exposed via `WorkflowResource` (see
  * `getMacroMapping`). Old entries are evicted by `evictAllForWorkflow` when a
  * workflow's executions finish — defensive against memory growth on
  * long-running deployments. The cache survives across compiles within the
  * SAME execution since the engine re-compiles internally on some paths.
  */
object MacroMappingCache {

  // The cache is written by ComputingUnitMaster's WorkflowCompiler when a run
  // starts, and read by TexeraWebApplication's REST endpoint when the
  // frontend polls. Those are SEPARATE JVMs, so an in-memory singleton
  // doesn't suffice. We back the cache with the local filesystem so both
  // processes see the same data.
  //
  // Layout (per workflow): /tmp/texera-macro-mappings/wid-{wid}.json
  // The file holds the most-recent compile's mapping; subsequent compiles
  // overwrite. eid-keyed history is omitted for now (the frontend always
  // wants "latest for this wid").
  //
  // In-memory cache is a fast-path; falls through to disk when missing.

  private val memCache =
    new ConcurrentHashMap[(WorkflowIdentity, ExecutionIdentity), Map[String, MacroProvenance]]()

  private val DiskDir = "/tmp/texera-macro-mappings"
  private val mapper =
    new ObjectMapper().registerModule(DefaultScalaModule)

  private def diskPathForWorkflow(wid: WorkflowIdentity): String =
    s"$DiskDir/wid-${wid.id}.json"

  def put(
      wid: WorkflowIdentity,
      eid: ExecutionIdentity,
      mapping: Map[String, MacroProvenance]
  ): Unit = {
    memCache.put((wid, eid), mapping)
    Try {
      Files.createDirectories(Paths.get(DiskDir))
      // Serialize as Map<String, {macroChain: List[String], bodyOpId: String}>
      val asJsonReady = mapping.map {
        case (k, v) =>
          k -> Map("macroChain" -> v.macroChain, "bodyOpId" -> v.bodyOpId)
      }
      val outFile = new File(diskPathForWorkflow(wid))
      Files.writeString(outFile.toPath, mapper.writeValueAsString(asJsonReady))
    }
  }

  /**
    * Look up a mapping for the latest known compile of (wid, eid). Returns an
    * empty map if no compile has happened yet — the frontend should poll
    * shortly after execution start.
    */
  def get(wid: WorkflowIdentity, eid: ExecutionIdentity): Map[String, MacroProvenance] =
    Option(memCache.get((wid, eid))).getOrElse(readFromDisk(wid))

  /**
    * Most recent mapping for a workflow id across all executions. Used by the
    * frontend when it doesn't know the exact eid yet (e.g. immediately after
    * clicking Run; the websocket hasn't confirmed eid yet).
    */
  def getLatestForWorkflow(wid: WorkflowIdentity): Map[String, MacroProvenance] = {
    import scala.jdk.CollectionConverters._
    val entries = memCache.entrySet().asScala.filter(_.getKey._1 == wid).toList
    val fromMem = entries.sortBy(-_.getKey._2.id).headOption.map(_.getValue)
    fromMem.getOrElse(readFromDisk(wid))
  }

  private def readFromDisk(wid: WorkflowIdentity): Map[String, MacroProvenance] = {
    val path = Paths.get(diskPathForWorkflow(wid))
    if (!Files.exists(path)) return Map.empty
    // Parse via Jackson tree API so we don't fight Scala/Java type erasure when
    // the DefaultScalaModule rewrites arrays to scala.List vs java.util.List.
    Try {
      val json = Files.readString(path)
      val root = mapper.readTree(json)
      import scala.jdk.CollectionConverters._
      val fields = root.fields().asScala.toList
      fields.map { entry =>
        val runtimeOpId = entry.getKey
        val node = entry.getValue
        val chainNode = node.get("macroChain")
        val chain =
          if (chainNode != null && chainNode.isArray)
            chainNode.elements().asScala.map(_.asText()).toList
          else Nil
        val bodyOpId = Option(node.get("bodyOpId")).map(_.asText()).getOrElse("")
        runtimeOpId -> MacroProvenance(chain, bodyOpId)
      }.toMap
    }.getOrElse(Map.empty)
  }

  def evictAllForWorkflow(wid: WorkflowIdentity): Unit = {
    import scala.jdk.CollectionConverters._
    val keysToRemove =
      memCache.keySet().asScala.filter(_._1 == wid).toList
    keysToRemove.foreach(memCache.remove)
    Try(Files.deleteIfExists(Paths.get(diskPathForWorkflow(wid))))
  }
}
