package edu.uci.ics.amber.core.storage

import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow.GlobalPortIdentity
import edu.uci.ics.amber.util.serde.GlobalPortIdentitySerde
import edu.uci.ics.amber.util.serde.GlobalPortIdentitySerde.SerdeOps

import java.net.URI

object VFSResourceType extends Enumeration {
  val RESULT: Value = Value("result")
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
      Option[GlobalPortIdentity],
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

    val globalPortIdOption = segments.indexOf("globalportid") match {
      case -1 => None
      case _  => Some(GlobalPortIdentitySerde.deserializeFromString(extractValue("globalportid")))
    }

    val resourceTypeStr = segments.last.toLowerCase
    val resourceType = VFSResourceType.values
      .find(_.toString.toLowerCase == resourceTypeStr)
      .getOrElse(throw new IllegalArgumentException(s"Unknown resource type: $resourceTypeStr"))

    (workflowId, executionId, globalPortIdOption, resourceType)
  }

  /**
    * Create a URI pointing to a result storage
    */
  def createResultURI(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      globalPortId: GlobalPortIdentity
  ): URI = {
    val baseUri =
      s"$VFS_FILE_URI_SCHEME:///wid/${workflowId.id}/eid/${executionId.id}/globalportid/${globalPortId.serializeAsString}"

    new URI(s"$baseUri/${VFSResourceType.RESULT.toString.toLowerCase}")
  }

  /**
    * Create a URI pointing to runtime statistics
    */
  def createRuntimeStatisticsURI(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): URI = {
    createNonResultVFSURI(
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
    createNonResultVFSURI(
      VFSResourceType.CONSOLE_MESSAGES,
      workflowId,
      executionId,
      Some(operatorId)
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
    * @throws IllegalArgumentException if `resourceType` is `RESULT`, if `operatorId` is provided for
    *                                  `RUNTIME_STATISTICS`, or if `operatorId` is not provided for `CONSOLE_MESSAGES`.
    */
  private def createNonResultVFSURI(
      resourceType: VFSResourceType.Value,
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorId: Option[OperatorIdentity] = None
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

    val baseUri = operatorId match {
      case Some(opId) =>
        s"$VFS_FILE_URI_SCHEME:///wid/${workflowId.id}/eid/${executionId.id}/opid/${opId.id}"
      case None => s"$VFS_FILE_URI_SCHEME:///wid/${workflowId.id}/eid/${executionId.id}"
    }

    new URI(s"$baseUri/${resourceType.toString.toLowerCase}")
  }
}
