package edu.uci.ics.amber.operator.source.scan.arrow

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.source.scan.ScanSourceOpDesc
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.util.ArrowUtils

import java.io.IOException
import java.net.URI
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import scala.util.Using

@JsonIgnoreProperties(value = Array("fileEncoding"))
class ArrowSourceOpDesc extends ScanSourceOpDesc {

  fileTypeName = Option("Arrow")

  @throws[IOException]
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) => createArrowSourceOpExec())
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> inferSchema()))
      )
  }

  private def createArrowSourceOpExec() = {
    new ArrowSourceOpExec(
      fileUri.get,
      limit,
      offset,
      schemaFunc = () => sourceSchema()
    )
  }

  /**
    * Infer Texera.Schema based on the top few lines of data.
    *
    * @return Texera.Schema build for this operator
    */
  @Override
  def inferSchema(): Schema = {
    val file = DocumentFactory.newReadonlyDocument(new URI(fileUri.get)).asFile()
    val allocator = new RootAllocator()

    Using
      .Manager { use =>
        val channel = use(Files.newByteChannel(file.toPath, StandardOpenOption.READ))
        val reader = use(new ArrowFileReader(channel, allocator))
        val arrowSchema: ArrowSchema = reader.getVectorSchemaRoot.getSchema
        ArrowUtils.toTexeraSchema(arrowSchema)
      }
      .getOrElse {
        throw new IOException("Failed to infer schema from Arrow file.")
      }
  }
}
