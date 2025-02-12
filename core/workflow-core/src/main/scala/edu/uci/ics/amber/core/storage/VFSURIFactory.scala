package edu.uci.ics.amber.core.storage

import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow.PortIdentity

import java.net.URI

object VFSResourceType extends Enumeration {
  val RESULT: Value = Value("result")
  val MATERIALIZED_RESULT: Value = Value("materializedResult")
  val RUNTIME_STATISTICS: Value = Value("runtimeStatistics")
  val CONSOLE_MESSAGES: Value = Value("consoleMessages")
}

object VFSURIFactory {
  val VFS_FILE_URI_SCHEME = "vfs"

  /**
    * Parses a VFS URI and extracts its components
    *
    * @param uri The VFS URI to parse.
    * @return A `VFSUriComponents` object with the extracted data.
    * @throws IllegalArgumentException if the URI is malformed.
    */
  def decodeURI(uri: URI): (
      WorkflowIdentity,
      ExecutionIdentity,
      Option[OperatorIdentity],
      Option[PortIdentity],
      VFSResourceType.Value
  ) = {
    if (uri.getScheme != VFS_FILE_URI_SCHEME) {
      throw new IllegalArgumentException(s"Invalid URI scheme: ${uri.getScheme}")
    }

    val segments = uri.getPath.stripPrefix("/").split("/").toList

    def extractValue(key: String): String = {
      val index = segments.indexOf(key)
      if (index == -1 || index + 1 >= segments.length) {
        throw new IllegalArgumentException(s"Missing value for key: $key in URI: $uri")
      }
      segments(index + 1)
    }

    val workflowId = WorkflowIdentity(extractValue("wid").toLong)
    val executionId = ExecutionIdentity(extractValue("eid").toLong)

    val operatorId = segments.indexOf("opid") match {
      case -1  => None
      case idx => Some(OperatorIdentity(extractValue("opid")))
    }

    val portIdentity: Option[PortIdentity] = segments.indexOf("pid") match {
      case -1 => None
      case idx if idx + 1 < segments.length =>
        val Array(portIdStr, portType) = segments(idx + 1).split("_")
        val portId = portIdStr.toInt
        val isInternal = portType match {
          case "I" => true
          case "E" => false
          case _   => throw new IllegalArgumentException(s"Invalid port type: $portType in URI: $uri")
        }
        Some(PortIdentity(portId, isInternal))
      case _ =>
        throw new IllegalArgumentException(s"Invalid port information in URI: $uri")
    }

    val resourceTypeStr = segments.last.toLowerCase
    val resourceType = VFSResourceType.values
      .find(_.toString.toLowerCase == resourceTypeStr)
      .getOrElse(throw new IllegalArgumentException(s"Unknown resource type: $resourceTypeStr"))

    (workflowId, executionId, operatorId, portIdentity, resourceType)
  }

  /**
    * Create a URI pointing to a result storage
    */
  def createResultURI(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorId: OperatorIdentity,
      portIdentity: PortIdentity
  ): URI = {
    createVFSURI(
      VFSResourceType.RESULT,
      workflowId,
      executionId,
      Some(operatorId),
      Some(portIdentity)
    )
  }

  /**
    * Create a URI pointing to a materialized storage
    */
  def createMaterializedResultURI(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorId: OperatorIdentity,
      portIdentity: PortIdentity
  ): URI = {
    createVFSURI(
      VFSResourceType.MATERIALIZED_RESULT,
      workflowId,
      executionId,
      Some(operatorId),
      Some(portIdentity)
    )
  }

  /**
    * Create a URI pointing to runtime statistics
    */
  def createRuntimeStatisticsURI(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): URI = {
    createVFSURI(
      VFSResourceType.RUNTIME_STATISTICS,
      workflowId,
      executionId
    )
  }

  /**
    * Create a URI pointing to console messages
    */
  def createConsoleMessagesURI(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorId: OperatorIdentity
  ): URI = {
    createVFSURI(
      VFSResourceType.CONSOLE_MESSAGES,
      workflowId,
      executionId,
      Some(operatorId)
    )
  }

  /**
    * Internal helper to create URI pointing to a VFS resource. The URI can be used by the DocumentFactory to create resource or open resource
    *
    * @param resourceType   The type of the VFS resource.
    * @param workflowId     Workflow identifier.
    * @param executionId    Execution identifier.
    * @param operatorId     Operator identifier.
    * @param portIdentity   Optional port identifier. **Required** if `resourceType` is `RESULT` or `MATERIALIZED_RESULT`.
    * @return A VFS URI
    * @throws IllegalArgumentException if `resourceType` is `RESULT` but `portIdentity` is missing.
    */
  private def createVFSURI(
      resourceType: VFSResourceType.Value,
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorId: Option[OperatorIdentity] = None,
      portIdentity: Option[PortIdentity] = None
  ): URI = {

    if (
      (resourceType == VFSResourceType.RESULT || resourceType == VFSResourceType.MATERIALIZED_RESULT) && (portIdentity.isEmpty || operatorId.isEmpty)
    ) {
      throw new IllegalArgumentException(
        "PortIdentity must be provided when resourceType is RESULT or MATERIALIZED_RESULT."
      )
    }

    if (
      resourceType == VFSResourceType.RUNTIME_STATISTICS && (operatorId.isDefined || portIdentity.isDefined)
    ) {
      throw new IllegalArgumentException(
        "Runtime statistics URI should not contain operatorId or portIdentity."
      )
    }

    if (
      resourceType == VFSResourceType.CONSOLE_MESSAGES && (operatorId.isEmpty || portIdentity.isDefined)
    ) {
      throw new IllegalArgumentException(
        "Console messages URI should contain operatorId."
      )
    }

    val baseUri = operatorId match {
      case Some(opId) =>
        s"$VFS_FILE_URI_SCHEME:///wid/${workflowId.id}/eid/${executionId.id}/opid/${opId.id}"
      case None => s"$VFS_FILE_URI_SCHEME:///wid/${workflowId.id}/eid/${executionId.id}"
    }

    val uriWithPort = portIdentity match {
      case Some(port) =>
        val portType = if (port.internal) "I" else "E"
        s"$baseUri/pid/${port.id}_$portType"
      case None =>
        baseUri
    }

    new URI(s"$uriWithPort/${resourceType.toString.toLowerCase}")
  }
}
