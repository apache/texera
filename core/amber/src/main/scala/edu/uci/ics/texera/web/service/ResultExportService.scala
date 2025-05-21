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

package edu.uci.ics.texera.web.service

import com.github.tototoshi.csv.CSVWriter
import edu.uci.ics.amber.core.storage.{DocumentFactory, EnvironmentalVariable}
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.util.ArrowUtils
import edu.uci.ics.texera.auth.JwtAuth
import edu.uci.ics.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_DAYS, dayToMin, jwtClaims}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.http.request.result.{OperatorExportInfo, ResultExportRequest}
import edu.uci.ics.texera.web.model.http.response.result.ResultExportResponse
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.{
  WorkflowExecutionsResource,
  WorkflowVersionResource
}
import edu.uci.ics.texera.web.service.WorkflowExecutionService.getLatestExecutionId

import java.io.{FilterOutputStream, IOException, OutputStream}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable
import scala.util.Using
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.commons.lang3.StringUtils

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.StreamingOutput
import java.net.{HttpURLConnection, URL, URLEncoder}
import scala.collection.mutable.ArrayBuffer

object Constants {
  val CHUNK_SIZE = 10
}

/**
  * A simple wrapper that ignores 'close()' calls on the underlying stream.
  * This allows each operator's writer to call close() without ending the entire ZipOutputStream.
  */
private class NonClosingOutputStream(os: OutputStream) extends FilterOutputStream(os) {
  @throws[IOException]
  override def close(): Unit = {
    // do not actually close the underlying stream
    super.flush()
    // omit super.close()
  }
}

object ResultExportService {
  lazy val fileServiceUploadOneFileToDatasetEndpoint: String =
    sys.env
      .getOrElse(
        EnvironmentalVariable.ENV_FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT,
        "http://localhost:9092/api/dataset/did/upload"
      )
      .trim
}

class ResultExportService(workflowIdentity: WorkflowIdentity) {

  import ResultExportService._

  /**
    * Export results for all specified operators in the request.
    */
  def exportAllOperatorsResultToDataset(
      user: User,
      request: ResultExportRequest
  ): ResultExportResponse = {
    val successMessages = new mutable.ListBuffer[String]()
    val errorMessages = new mutable.ListBuffer[String]()

    request.operators.foreach { op =>
      try {
        val (msgOpt, errOpt) = exportSingleOperatorToDataset(user, request, op)
        msgOpt.foreach(successMessages += _)
        errOpt.foreach(errorMessages += _)
      } catch {
        case ex: Exception =>
          errorMessages += s"Error exporting operator $op: ${ex.getMessage}"
      }
    }

    if (errorMessages.isEmpty) {
      ResultExportResponse("success", successMessages.mkString("\n"))
    } else if (successMessages.isEmpty) {
      ResultExportResponse("error", errorMessages.mkString("\n"))
    } else {
      // At least one success, so we consider overall success (with partial possible).
      ResultExportResponse("success", successMessages.mkString("\n"))
    }
  }

  /**
    * Export a single operator's result and handle different export types.
    */
  private def exportSingleOperatorToDataset(
      user: User,
      request: ResultExportRequest,
      operatorRequest: OperatorExportInfo
  ): (Option[String], Option[String]) = {

    val execIdOpt = getLatestExecutionId(workflowIdentity)
    if (execIdOpt.isEmpty)
      return (None, Some(s"Workflow ${request.workflowId} has no execution result"))

    val operatorDocument = getOperatorDocument(operatorRequest.id)
    if (operatorDocument == null || operatorDocument.getCount == 0)
      return (None, Some(s"No results to export for operator $operatorRequest"))

    val attributeNames =
      operatorDocument.getRange(0, 1).to(Iterable).head.getSchema.getAttributeNames // small cost

    val writer: OutputStream => Unit = operatorRequest.outputType match {
      case "csv"   => out => streamDocumentAsCSV(operatorDocument, out, Some(attributeNames))
      case "arrow" => out => streamDocumentAsArrow(operatorDocument, out)
      case "html"  => out => streamDocumentAsHTML(out, operatorDocument)
      case "data"  => out => streamCellData(out, request, operatorDocument)
      case _       => out => streamDocumentAsCSV(operatorDocument, out, Some(attributeNames))
    }

    saveStreamToDataset(
      operatorId = operatorRequest.id,
      user = user,
      request = request,
      extension = operatorRequest.outputType,
      writer = writer
    )
  }

  /**
    * Export a single operator's results as a streaming response (e.g., for download).
    */
  def exportOperatorResultAsStream(
      request: ResultExportRequest,
      operatorRequest: OperatorExportInfo
  ): (StreamingOutput, Option[String]) = {
    val execIdOpt = getLatestExecutionId(workflowIdentity)
    if (execIdOpt.isEmpty) {
      return (null, None)
    }

    val operatorDocument = getOperatorDocument(operatorRequest.id)
    if (operatorDocument == null || operatorDocument.getCount == 0) {
      return (null, None)
    }

    val fileName =
      if (request.filename.isEmpty)
        generateFileName(request, operatorRequest.id, operatorRequest.outputType)
      else request.filename

    val streamingOutput: StreamingOutput = (out: OutputStream) => {
      operatorRequest.outputType match {
        case "csv"   => streamDocumentAsCSV(operatorDocument, out, None)
        case "arrow" => streamDocumentAsArrow(operatorDocument, out)
        case "data"  => streamCellData(out, request, operatorDocument) // handle single cell export
        case "html" =>
          streamDocumentAsHTML(
            out,
            operatorDocument
          ) // handle HTML export for visualization operators
        case _ => streamDocumentAsCSV(operatorDocument, out, None) // fallback
      }
    }

    (streamingOutput, Some(fileName))
  }

  /**
    * Export multiple operators' results as a single ZIP file stream.
    */
  def exportOperatorsAsZip(
      request: ResultExportRequest
  ): (StreamingOutput, Option[String]) = {
    if (request.operators.isEmpty) {
      return (null, None)
    }

    val timestamp = LocalDateTime
      .now()
      .truncatedTo(ChronoUnit.SECONDS)
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))
    val zipFileName = s"${request.workflowName}-$timestamp.zip"

    val execIdOpt = getLatestExecutionId(workflowIdentity)
    if (execIdOpt.isEmpty) {
      throw new WebApplicationException(
        s"No execution result for workflow ${request.workflowId}"
      )
    }

    val streamingOutput: StreamingOutput = new StreamingOutput {
      override def write(outputStream: OutputStream): Unit = {
        Using.resource(new ZipOutputStream(outputStream)) { zipOut =>
          request.operators.foreach { op =>
            val operatorDocument = getOperatorDocument(op.id)
            if (operatorDocument == null || operatorDocument.getCount == 0) {
              // create an "empty" file for this operator
              zipOut.putNextEntry(new ZipEntry(s"${op.id}-empty.txt"))
              val msg = s"Operator ${op.id} has no results"
              zipOut.write(msg.getBytes(StandardCharsets.UTF_8))
              zipOut.closeEntry()
            } else {
              val operatorFileName = generateFileName(request, op.id, op.outputType)

              zipOut.putNextEntry(new ZipEntry(operatorFileName))
              val nonClosingStream = new NonClosingOutputStream(zipOut)

              op.outputType match {
                case "csv"   => streamDocumentAsCSV(operatorDocument, nonClosingStream, None)
                case "arrow" => streamDocumentAsArrow(operatorDocument, nonClosingStream)
                case "data" =>
                  streamCellData(
                    nonClosingStream,
                    request,
                    operatorDocument
                  ) // handle single cell export
                case "html" =>
                  streamDocumentAsHTML(
                    nonClosingStream,
                    operatorDocument
                  ) // handle HTML export for visualization operators
                case _ => streamDocumentAsCSV(operatorDocument, nonClosingStream, None)
              }
              zipOut.closeEntry()
            }
          }
        }
      }
    }

    (streamingOutput, Some(zipFileName))
  }

  /**
    * Streams the entire content of `VirtualDocument` as CSV into `outputStream` in a single pass.
    */
  private def streamDocumentAsCSV(
      doc: VirtualDocument[Tuple],
      outputStream: OutputStream,
      maybeHeaders: Option[List[String]]
  ): Unit = {
    val totalCount = doc.getCount
    if (totalCount == 0) {
      return
    }

    val iterator = doc.get()
    if (!iterator.hasNext) {
      return
    }

    val csvWriter = CSVWriter.open(outputStream)

    val headers: List[String] = maybeHeaders match {
      case Some(hdrs) =>
        hdrs
      case None =>
        val firstRow = iterator.next()
        val inferredHeaders = firstRow.getSchema.getAttributeNames

        csvWriter.writeRow(inferredHeaders)
        csvWriter.writeRow(firstRow.getFields.toIndexedSeq)

        inferredHeaders
    }

    if (maybeHeaders.isDefined) {
      csvWriter.writeRow(headers)
    }

    val buffer = new ArrayBuffer[Tuple](Constants.CHUNK_SIZE)

    while (iterator.hasNext) {
      buffer.clear()
      var count = 0

      while (count < Constants.CHUNK_SIZE && iterator.hasNext) {
        buffer += iterator.next()
        count += 1
      }
      buffer.foreach { t =>
        csvWriter.writeRow(t.getFields.toIndexedSeq)
      }
      csvWriter.flush()
    }

    csvWriter.close()
  }

  /**
    * Streams the entire content of `VirtualDocument` as Arrow into `outputStream` in a single pass.
    */
  private def streamDocumentAsArrow(
      doc: VirtualDocument[Tuple],
      outputStream: OutputStream
  ): Unit = {
    if (doc.getCount == 0) return

    val allocator = new RootAllocator()
    Using.Manager { use =>
      val firstTuple = doc.getRange(0, 1).to(Iterable).head
      val schema = firstTuple.getSchema
      val arrowSchema = ArrowUtils.fromTexeraSchema(schema)

      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      use(root)

      val channel = Channels.newChannel(outputStream)
      val writer = new ArrowFileWriter(root, null, channel)
      use(writer)
      use(allocator)

      writer.start()

      val iterator = doc.get()
      val buffer = new ArrayBuffer[Tuple](Constants.CHUNK_SIZE)

      while (iterator.hasNext) {
        buffer.clear()
        var count = 0

        while (count < Constants.CHUNK_SIZE && iterator.hasNext) {
          buffer += iterator.next()
          count += 1
        }

        if (buffer.nonEmpty) {
          val currentBatchSize = buffer.size

          for (i <- 0 until currentBatchSize) {
            val tuple = buffer(i)
            ArrowUtils.setTexeraTuple(tuple, i, root)
          }

          root.setRowCount(currentBatchSize)
          writer.writeBatch()

          root.clear()
        }
      }

      writer.end()
    }
  }

  /*
   * Handle streaming HTML result from a visualization operator's result.
   */
  private def streamDocumentAsHTML(
      out: OutputStream,
      operatorDocument: VirtualDocument[Tuple]
  ): Unit = {
    val results: Iterable[Tuple] = operatorDocument.get().to(Iterable)
    val resHead = results.head
    val htmlCode = resHead.getField(0).toString
    out.write(htmlCode.getBytes(StandardCharsets.UTF_8))
    out.flush()
  }

  /*
   * Handle streaming a single (row, column) from an operator's result.
   * This is used for the "data" export type, which exports a single field value.
   */
  private def streamCellData(
      out: OutputStream,
      request: ResultExportRequest,
      operatorDocument: VirtualDocument[Tuple]
  ): Unit = {
    val rowIndex = request.rowIndex
    val columnIndex = request.columnIndex

    if (rowIndex >= operatorDocument.getCount) {
      throw new WebApplicationException(
        s"Invalid rowIndex ($rowIndex). Total rows: ${operatorDocument.getCount}"
      )
    }

    val selectedRow = operatorDocument
      .getRange(rowIndex, rowIndex + 1)
      .to(Iterable)
      .headOption
      .getOrElse(throw new RuntimeException(s"Could not retrieve row at index $rowIndex"))

    if (columnIndex >= selectedRow.getFields.length) {
      throw new WebApplicationException(
        s"Invalid columnIndex ($columnIndex). Total columns: ${selectedRow.getFields.length}"
      )
    }

    val field: Any = selectedRow.getField(columnIndex)
    val dataBytes = convertFieldToBytes(field)
    out.write(dataBytes)
  }

  /**
    * Generate the VirtualDocument for one operator's result.
    * Incorporates the remote code's extra parameter `None` for sub-operator ID.
    */
  private def getOperatorDocument(operatorId: String): VirtualDocument[Tuple] = {
    // By now the workflow should finish running
    // Only supports external port 0 for now. TODO: support multiple ports
    val storageUri = WorkflowExecutionsResource.getResultUriByLogicalPortId(
      getLatestExecutionId(workflowIdentity).get,
      OperatorIdentity(operatorId),
      PortIdentity()
    )

    // Return null if no URI so that caller can handle empty/absent data
    storageUri
      .map(uri => DocumentFactory.openDocument(uri)._1.asInstanceOf[VirtualDocument[Tuple]])
      .orNull
  }

  private def saveStreamToDataset(
      operatorId: String,
      user: User,
      request: ResultExportRequest,
      extension: String,
      writer: OutputStream => Unit
  ): (Option[String], Option[String]) = {
    val fileName =
      if (request.filename.isEmpty) generateFileName(request, operatorId, extension)
      else request.filename

    try {
      saveToDatasets(request, user, writer, fileName)
      (Some(s"$extension export done for operator $operatorId -> file: $fileName"), None)
    } catch {
      case ex: Exception =>
        (None, Some(s"$extension export failed for operator $operatorId: ${ex.getMessage}"))
    }
  }

  private def convertFieldToBytes(field: Any): Array[Byte] = {
    field match {
      case data: Array[Byte] => data
      case data: String      => data.getBytes(StandardCharsets.UTF_8)
      case other             => other.toString.getBytes(StandardCharsets.UTF_8)
    }
  }

  /**
    * Save the pipedInputStream into the specified datasets as a new dataset version.
    */
  private def saveToDatasets(
      request: ResultExportRequest,
      user: User,
      fileWriter: OutputStream => Unit, // Pass function that writes data
      fileName: String
  ): Unit = {
    request.datasetIds.foreach { did =>
      val encodedFilePath = URLEncoder.encode(fileName, StandardCharsets.UTF_8.name())
      val message = URLEncoder.encode(
        s"Export from workflow ${request.workflowName}",
        StandardCharsets.UTF_8.name()
      )

      val uploadUrl = s"$fileServiceUploadOneFileToDatasetEndpoint"
        .replace("did", did.toString) + s"?filePath=$encodedFilePath&message=$message"

      var connection: HttpURLConnection = null
      try {
        val url = new URL(uploadUrl)
        connection = url.openConnection().asInstanceOf[HttpURLConnection]
        connection.setDoOutput(true)
        connection.setRequestMethod("POST")
        connection.setRequestProperty("Content-Type", "application/octet-stream")
        connection.setRequestProperty(
          "Authorization",
          s"Bearer ${JwtAuth.jwtToken(jwtClaims(user, dayToMin(TOKEN_EXPIRE_TIME_IN_DAYS)))}"
        )
        connection.setChunkedStreamingMode(0)

        // Get output stream from connection
        val outputStream = connection.getOutputStream
        fileWriter(outputStream) // Write directly to HTTP request output stream
        outputStream.close()

        // Check response
        val responseCode = connection.getResponseCode
        if (responseCode != HttpURLConnection.HTTP_OK) {
          throw new RuntimeException(s"Failed to upload file. Server responded with: $responseCode")
        }
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"Error uploading file to dataset $did: ${e.getMessage}", e)
      } finally {
        if (connection != null) connection.disconnect()
      }
    }
  }

  /**
    * Generate a file name for an operator's exported file.
    * Preserves your logic: uses operatorId in the name.
    */
  private def generateFileName(
      request: ResultExportRequest,
      operatorId: String,
      extension: String
  ): String = {
    val latestVersion =
      WorkflowVersionResource.getLatestVersion(request.workflowId)
    val timestamp = LocalDateTime
      .now()
      .truncatedTo(ChronoUnit.SECONDS)
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))

    val rawName = s"${request.workflowName}-op$operatorId-v$latestVersion-$timestamp.$extension"
    // remove path separators
    StringUtils.replaceEach(rawName, Array("/", "\\"), Array("", ""))
  }
}
