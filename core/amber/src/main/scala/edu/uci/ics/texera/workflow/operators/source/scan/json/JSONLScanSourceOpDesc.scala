package edu.uci.ics.texera.workflow.operators.source.scan.json

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.JsonNode
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.model.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.engine.common.storage.{DatasetFileDocument, DocumentFactory}
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.Utils.objectMapper
import edu.uci.ics.amber.engine.common.model.tuple.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.amber.engine.common.model.tuple.{Attribute, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONUtil.JSONToMap

import java.io.{BufferedReader, FileInputStream, IOException, InputStream, InputStreamReader}
import java.net.URI
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala

class JSONLScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(required = true)
  @JsonPropertyDescription("flatten nested objects and arrays")
  var flatten: Boolean = false

  fileTypeName = Option("JSONL")

  def createInputStream(filepath: String, fileDesc: DatasetFileDocument): InputStream = {
    if (filepath != null) {
      new FileInputStream(filepath)
    } else {
      fileDesc.asInputStream()
    }
  }

  @throws[IOException]
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val stream = DocumentFactory.newReadonlyDocument(new URI(fileUri.get)).asInputStream()
    // count lines and partition the task to each worker
    val reader = new BufferedReader(
      new InputStreamReader(stream, fileEncoding.getCharset)
    )
    val offsetValue = offset.getOrElse(0)
    var lines = reader.lines().iterator().asScala.drop(offsetValue)
    if (limit.isDefined) lines = lines.take(limit.get)
    val count: Int = lines.map(_ => 1).sum
    reader.close()

    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((idx, workerCount) => {
          val startOffset: Int = offsetValue + count / workerCount * idx
          val endOffset: Int =
            offsetValue + (if (idx != workerCount - 1) count / workerCount * (idx + 1)
                           else count)
          new JSONLScanSourceOpExec(
            fileUri.get,
            fileEncoding,
            startOffset,
            endOffset,
            flatten,
            schemaFunc = () => inferSchema()
          )
        })
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(true)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> inferSchema()))
      )
  }

  /**
    * Infer Texera.Schema based on the top few lines of data.
    *
    * @return Texera.Schema build for this operator
    */
  @Override
  def inferSchema(): Schema = {
    if (fileUri.isEmpty) {
      return null
    }
    val stream = DocumentFactory.newReadonlyDocument(new URI(fileUri.get)).asInputStream()
    val reader = new BufferedReader(new InputStreamReader(stream, fileEncoding.getCharset))
    var fieldNames = Set[String]()

    val allFields: ArrayBuffer[Map[String, String]] = ArrayBuffer()

    val startOffset = offset.getOrElse(0)
    val endOffset =
      startOffset + limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
    reader
      .lines()
      .iterator()
      .asScala
      .slice(startOffset, endOffset)
      .foreach(line => {
        val root: JsonNode = objectMapper.readTree(line)
        if (root.isObject) {
          val fields: Map[String, String] = JSONToMap(root, flatten = flatten)
          fieldNames = fieldNames.++(fields.keySet)
          allFields += fields
        }
      })

    val sortedFieldNames = fieldNames.toList.sorted
    reader.close()

    val attributeTypes = inferSchemaFromRows(allFields.iterator.map(fields => {
      val result = ArrayBuffer[Object]()
      for (fieldName <- sortedFieldNames) {
        if (fields.contains(fieldName)) {
          result += fields(fieldName)
        } else {
          result += null
        }
      }
      result.toArray
    }))

    Schema
      .builder()
      .add(
        sortedFieldNames.indices
          .map(i => new Attribute(sortedFieldNames(i), attributeTypes(i)))
      )
      .build()
  }

}
