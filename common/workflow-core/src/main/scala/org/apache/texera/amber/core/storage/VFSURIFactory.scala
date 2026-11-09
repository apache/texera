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

package org.apache.texera.amber.core.storage

import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.GlobalPortIdentity
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps

import java.net.URI

object VFSResourceType extends Enumeration {
  val RESULT: Value = Value("result")
  val RUNTIME_STATISTICS: Value = Value("runtimeStatistics")
  val CONSOLE_MESSAGES: Value = Value("consoleMessages")
  val STATE: Value = Value("state")
}

/**
  * The named components encoded in a VFS URI, as returned by
  * [[VFSURIFactory.decodeURI]].
  */
case class VFSUriComponents(
    workflowId: WorkflowIdentity,
    executionId: ExecutionIdentity,
    globalPortId: Option[GlobalPortIdentity],
    resourceType: VFSResourceType.Value,
    // The warehouse whose catalog holds this URI's table, from the optional leading
    // `/wh/<name>` segment; None for non-BYO storage, which uses the configured
    // default. Last so positional unpacking of the earlier fields still works.
    warehouse: Option[String] = None
)

object VFSURIFactory {
  val VFS_FILE_URI_SCHEME = "vfs"

  // A warehouse name becomes a URI path segment, so it is restricted to characters
  // that carry no meaning there: no '/' to add segments, and no '%' to smuggle one
  // in percent-encoded form. Registration applies a stricter rule still; this is the
  // invariant the URI layer itself depends on.
  private val warehouseNamePattern = "[A-Za-z0-9][A-Za-z0-9_-]*".r

  /**
    * Whether `name` is safe as a URI path segment. Public so registration-time
    * validation (WarehouseResource) enforces the exact rule this layer parses by,
    * instead of keeping a drifting copy.
    */
  def isValidWarehouseName(name: String): Boolean =
    warehouseNamePattern.pattern.matcher(name).matches()

  // Warehouse is carried as a leading `/wh/<name>` path segment so a storage URI
  // fully identifies which warehouse its table lives in. Absent for non-BYO storage.
  private def warehousePathSegment(warehouse: Option[String]): String =
    warehouse
      .map { name =>
        require(
          isValidWarehouseName(name),
          s"warehouse name must match ${warehouseNamePattern.regex} " +
            s"(it becomes a URI path segment): $name"
        )
        s"/wh/$name"
      }
      .getOrElse("")

  /**
    * The warehouse encoded in a VFS URI, if present. Reported as part of
    * [[VFSUriComponents]] by [[decodeURI]], which is the only way in.
    *
    * Anchored to the leading segment on purpose. The warehouse is written as a
    * leading `/wh/<name>` prefix and `DocumentFactory` strips only a leading one,
    * so scanning the whole path would disagree with the stripper: a later segment
    * that happens to be `wh` -- e.g. inside a user-chosen operator id in a
    * console-messages URI -- would select a warehouse the URI was never built for.
    *
    * The segments come from the RAW path (see `decodeURI`), so a percent-encoded
    * slash stays inside the segment that contains it instead of becoming a
    * separator. The name is then required to be a legal warehouse name, so anything
    * that could not have been written by `warehousePathSegment` resolves to no
    * warehouse rather than to a wrong one.
    */
  private def warehouseFrom(segments: List[String]): Option[String] =
    segments match {
      case "wh" :: name :: _ if isValidWarehouseName(name) => Some(name)
      case _                                               => None
    }

  /**
    * Parses a VFS URI and extracts its components
    *
    * @param uri The VFS URI to parse.
    * @return A `VFSUriComponents` object with the extracted data.
    * @throws java.lang.IllegalArgumentException if the URI is malformed.
    */
  def decodeURI(uri: URI): VFSUriComponents = {
    if (uri.getScheme != VFS_FILE_URI_SCHEME) {
      throw new IllegalArgumentException(s"Invalid URI scheme: ${uri.getScheme}")
    }

    // Raw path, for the same reason as warehouseFromURI: keys are located by
    // searching the segments, so a percent-encoded slash inside a segment must not
    // split it and shift which `wid`/`eid` the search finds. Python's decode_uri
    // splits the raw path too, so both languages read a URI identically.
    val segments = uri.getRawPath.stripPrefix("/").split("/").toList

    def extractValue(key: String): String = {
      val index = segments.indexOf(key)
      if (index == -1 || index + 1 >= segments.length) {
        throw new IllegalArgumentException(s"Missing value for key: $key in URI: $uri")
      }
      segments(index + 1)
    }

    val workflowId = WorkflowIdentity(extractValue("wid").toLong)
    val executionId = ExecutionIdentity(extractValue("eid").toLong)

    val globalPortIdOption = segments.indexOf("globalportid") match {
      case -1 => None
      case _  => Some(GlobalPortIdentitySerde.deserializeFromString(extractValue("globalportid")))
    }

    val resourceTypeStr = segments.last.toLowerCase
    val resourceType = VFSResourceType.values
      .find(_.toString.toLowerCase == resourceTypeStr)
      .getOrElse(throw new IllegalArgumentException(s"Unknown resource type: $resourceTypeStr"))

    VFSUriComponents(
      workflowId,
      executionId,
      globalPortIdOption,
      resourceType,
      warehouseFrom(segments)
    )
  }

  /**
    * Create the base URI for a port. Result and state URIs are derived
    * from this base via `resultURI` / `stateURI`.
    */
  def createPortBaseURI(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      globalPortId: GlobalPortIdentity,
      warehouse: Option[String] = None
  ): URI =
    new URI(
      s"$VFS_FILE_URI_SCHEME://${warehousePathSegment(warehouse)}/wid/${workflowId.id}/eid/${executionId.id}" +
        s"/globalportid/${globalPortId.serializeAsString}"
    )

  def resultURI(baseURI: URI): URI = appendResource(baseURI, VFSResourceType.RESULT)

  def stateURI(baseURI: URI): URI = appendResource(baseURI, VFSResourceType.STATE)

  private def appendResource(baseURI: URI, resourceType: VFSResourceType.Value): URI =
    new URI(s"$baseURI/${resourceType.toString.toLowerCase}")

  /**
    * Create a URI pointing to runtime statistics
    */
  def createRuntimeStatisticsURI(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      warehouse: Option[String] = None
  ): URI = {
    createNonResultVFSURI(
      VFSResourceType.RUNTIME_STATISTICS,
      workflowId,
      executionId,
      warehouse = warehouse
    )
  }

  /**
    * Create a URI pointing to console messages
    */
  def createConsoleMessagesURI(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorId: OperatorIdentity,
      warehouse: Option[String] = None
  ): URI = {
    createNonResultVFSURI(
      VFSResourceType.CONSOLE_MESSAGES,
      workflowId,
      executionId,
      Some(operatorId),
      warehouse
    )
  }

  /**
    * Internal helper to create URI pointing to a VFS resource for resource types other than `RESULT`.
    * The URI can be used by the DocumentFactory to create resource or open resource.
    *
    * @param resourceType   The type of the VFS resource.
    * @param workflowId     Workflow identifier.
    * @param executionId    Execution identifier.
    * @param operatorId     Operator identifier.
    * @return A VFS URI
    * @throws java.lang.IllegalArgumentException if `resourceType` is `RESULT`, if `operatorId` is provided for
    *                                  `RUNTIME_STATISTICS`, or if `operatorId` is not provided for `CONSOLE_MESSAGES`.
    */
  private def createNonResultVFSURI(
      resourceType: VFSResourceType.Value,
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorId: Option[OperatorIdentity] = None,
      warehouse: Option[String] = None
  ): URI = {

    if (resourceType == VFSResourceType.RESULT) {
      throw new IllegalArgumentException(
        "resourceType cannot be RESULT when using createOtherVFSURI."
      )
    }

    if (resourceType == VFSResourceType.RUNTIME_STATISTICS && operatorId.isDefined) {
      throw new IllegalArgumentException(
        "Runtime statistics URI should not contain operatorId."
      )
    }

    if (resourceType == VFSResourceType.CONSOLE_MESSAGES && operatorId.isEmpty) {
      throw new IllegalArgumentException(
        "Console messages URI should contain operatorId."
      )
    }

    // The operator id is user-supplied (it comes straight off the workflow JSON) and
    // is interpolated into the URI path below. A '/' in it would add path segments,
    // letting it forge structure the URI never meant to have -- e.g. a `wh/<name>`
    // pair that would then be read back as a warehouse.
    operatorId.foreach { opId =>
      require(
        !opId.id.contains('/'),
        s"operatorId must not contain '/' (VFS URI parsing relies on this): ${opId.id}"
      )
    }

    val whSegment = warehousePathSegment(warehouse)
    val baseUri = operatorId match {
      case Some(opId) =>
        s"$VFS_FILE_URI_SCHEME://$whSegment/wid/${workflowId.id}/eid/${executionId.id}/opid/${opId.id}"
      case None => s"$VFS_FILE_URI_SCHEME://$whSegment/wid/${workflowId.id}/eid/${executionId.id}"
    }

    new URI(s"$baseUri/${resourceType.toString.toLowerCase}")
  }
}
