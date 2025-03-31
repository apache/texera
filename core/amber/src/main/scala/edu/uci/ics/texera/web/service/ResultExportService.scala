package edu.uci.ics.texera.web.service

import com.github.tototoshi.csv.CSVWriter
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.util.ArrowUtils
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.request.ResultExportRequest
import edu.uci.ics.texera.web.model.websocket.response.ResultExportResponse
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
import java.util.concurrent.{Executors, ThreadPoolExecutor}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable
import scala.util.Using
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.commons.lang3.StringUtils
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.StreamingOutput
import edu.uci.ics.texera.web.auth.JwtAuth
import edu.uci.ics.texera.web.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_DAYS, dayToMin, jwtClaims}

import java.net.{HttpURLConnection, URL, URLEncoder}

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

  // Matches the remote's approach for a thread pool
  final private val pool: ThreadPoolExecutor =
    Executors.newFixedThreadPool(3).asInstanceOf[ThreadPoolExecutor]

  lazy val fileServiceUploadOneFileToDatasetEndpoint: String =
    sys.env
      .getOrElse(
        "FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT",
        "http://localhost:9092/api/dataset/did/upload"
      )
      .trim
}

class ResultExportService(workflowIdentity: WorkflowIdentity) {

  import ResultExportService._

  /**
    * Generate the VirtualDocument for one operator's result.
    * Incorporates the remote code's extra parameter `None` for sub-operator ID.
    */
  private def generateOneOperatorResult(operatorId: String): VirtualDocument[Tuple] = {
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

  /**
    * Export results for all specified operators in the request.
    */
  def exportResult(user: User, request: ResultExportRequest): ResultExportResponse = {
    val successMessages = new mutable.ListBuffer[String]()
    val errorMessages = new mutable.ListBuffer[String]()

    // Handle each operator requested
    request.operatorIds.foreach { opId =>
      try {
        val (msgOpt, errOpt) = exportSingleOperator(user, request, opId)
        msgOpt.foreach(successMessages += _)
        errOpt.foreach(errorMessages += _)
      } catch {
        case ex: Exception =>
          errorMessages += s"Error exporting operator $opId: ${ex.getMessage}"
      }
    }

    // Decide final response
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
  private def exportSingleOperator(
      user: User,
      request: ResultExportRequest,
      operatorId: String
  ): (Option[String], Option[String]) = {

    val execIdOpt = getLatestExecutionId(workflowIdentity)
    if (execIdOpt.isEmpty) {
      return (None, Some(s"Workflow ${request.workflowId} has no execution result"))
    }

    val operatorResult = generateOneOperatorResult(operatorId)
    if (operatorResult == null || operatorResult.getCount == 0) {
      return (Some("error"), Some("The workflow contains no results"))
    }

    val results = operatorResult.get().to(Iterable)
    val attributeNames = results.head.getSchema.getAttributeNames

    request.exportType match {
      case "csv" =>
        handleCSVRequest(operatorId, user, request, results, attributeNames)

      case "data" =>
        handleDataRequest(operatorId, user, request, results)

      case "arrow" =>
        handleArrowRequest(operatorId, user, request, results)

      case unknown =>
        (None, Some(s"Unknown export type: $unknown"))
    }
  }

  /**
    * Handle exporting a CSV file for a single operator.
    */
  private def handleCSVRequest(
      operatorId: String,
      user: User,
      request: ResultExportRequest,
      results: Iterable[Tuple],
      headers: List[String]
  ): (Option[String], Option[String]) = {
    val fileName = generateFileName(request, operatorId, "csv")
    try {

      saveToDatasets(
        request,
        user,
        outputStream => {
          val writer = CSVWriter.open(outputStream)
          writer.writeRow(headers)
          results.foreach { tuple =>
            writer.writeRow(tuple.getFields.toIndexedSeq)
          }
          writer.close()
        },
        fileName
      )
      (Some(s"CSV export done for operator $operatorId -> file: $fileName"), None)
    } catch {
      case ex: Exception =>
        (None, Some(s"CSV export failed for operator $operatorId: ${ex.getMessage}"))
    }
  }

  /*
   * Handle streaming a single (row, column) from an operator's result.
   * This is used for the "data" export type, which exports a single field value.
   */
  private def writeData(
      out: OutputStream,
      request: ResultExportRequest,
      results: Iterable[Tuple]
  ): Unit = {
    val rowIndex = request.rowIndex
    val columnIndex = request.columnIndex

    if (rowIndex >= results.size || columnIndex >= results.head.getFields.length) {
      -1
    }

    val selectedRow = results.toSeq(rowIndex)
    val field: Any = selectedRow.getField(columnIndex)
    val dataBytes = convertFieldToBytes(field)
    out.write(dataBytes)
  }

  /**
    * Handle exporting data for a single (row, column) from an operator's result.
    */
  private def handleDataRequest(
      operatorId: String,
      user: User,
      request: ResultExportRequest,
      results: Iterable[Tuple]
  ): (Option[String], Option[String]) = {
    try {
      val rowIndex = request.rowIndex
      val columnIndex = request.columnIndex
      val fileName = request.filename

      if (rowIndex >= results.size || columnIndex >= results.head.getFields.length) {
        return (None, Some(s"Invalid rowIndex or columnIndex for operator $operatorId"))
      }

      val selectedRow = results.toSeq(rowIndex)
      val field: Any = selectedRow.getField(columnIndex)
      val dataBytes: Array[Byte] = convertFieldToBytes(field)

      saveToDatasets(
        request,
        user,
        outputStream => {
          outputStream.write(dataBytes)
          outputStream.close()
        },
        fileName
      )
      (Some(s"Data export done for operator $operatorId -> file: $fileName"), None)
    } catch {
      case ex: Exception =>
        (None, Some(s"Data export failed for operator $operatorId: ${ex.getMessage}"))
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
    * Handle exporting results to Arrow format for a single operator.
    */
  private def handleArrowRequest(
      operatorId: String,
      user: User,
      request: ResultExportRequest,
      results: Iterable[Tuple]
  ): (Option[String], Option[String]) = {
    if (results.isEmpty) {
      return (None, Some(s"No results to export for operator $operatorId"))
    }

    try {
      val fileName = generateFileName(request, operatorId, "arrow")

      saveToDatasets(
        request,
        user,
        outputStream => {
          val allocator = new RootAllocator()
          Using.Manager { use =>
            val (writer, root) = createArrowWriter(results, allocator, outputStream)
            use(writer)
            use(root)
            use(allocator)

            writeArrowData(writer, root, results)
          }
        },
        fileName
      )

      (Some(s"Arrow file export done for operator $operatorId -> file: $fileName"), None)
    } catch {
      case ex: Exception =>
        (None, Some(s"Arrow export failed for operator $operatorId: ${ex.getMessage}"))
    }
  }

  private def createArrowWriter(
      results: Iterable[Tuple],
      allocator: RootAllocator,
      outputStream: OutputStream
  ): (ArrowFileWriter, VectorSchemaRoot) = {
    val schema = results.head.getSchema
    val arrowSchema = ArrowUtils.fromTexeraSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val channel = Channels.newChannel(outputStream)
    val writer = new ArrowFileWriter(root, null, channel)
    (writer, root)
  }

  private def writeArrowData(
      writer: ArrowFileWriter,
      root: VectorSchemaRoot,
      results: Iterable[Tuple]
  ): Unit = {
    writer.start()
    val batchSize = 1000
    val resultList = results.toList
    val totalSize = resultList.size

    for (batchStart <- 0 until totalSize by batchSize) {
      val batchEnd = Math.min(batchStart + batchSize, totalSize)
      val currentBatchSize = batchEnd - batchStart

      for (i <- 0 until currentBatchSize) {
        val tuple = resultList(batchStart + i)
        ArrowUtils.setTexeraTuple(tuple, i, root)
      }
      root.setRowCount(currentBatchSize)
      writer.writeBatch()
      root.clear()
    }
    writer.end()
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
    * Export a single operator's results as a streaming response (e.g., for download).
    */
  def exportOperatorResultAsStream(
      request: ResultExportRequest,
      operatorId: String
  ): (StreamingOutput, Option[String]) = {
    val execIdOpt = getLatestExecutionId(workflowIdentity)
    if (execIdOpt.isEmpty) {
      return (null, None)
    }

    val operatorResult = generateOneOperatorResult(operatorId)
    if (operatorResult == null || operatorResult.getCount == 0) {
      return (null, None)
    }

    val results: Iterable[Tuple] = operatorResult.get().to(Iterable)
    val extension: String = request.exportType match {
      case "csv"   => "csv"
      case "arrow" => "arrow"
      case "data"  => "bin"
      case _       => "dat"
    }

    val fileName = generateFileName(request, operatorId, extension)

    val streamingOutput: StreamingOutput = new StreamingOutput {
      override def write(out: OutputStream): Unit = {
        request.exportType match {
          case "csv"   => writeCsv(out, results)
          case "arrow" => writeArrow(out, results)
          case "data"  => writeData(out, request, results) // handle single cell export
          case _       => writeCsv(out, results) // fallback
        }
      }
    }

    (streamingOutput, Some(fileName))
  }

  private def writeCsv(outputStream: OutputStream, results: Iterable[Tuple]): Unit = {
    if (results.isEmpty) return
    val csvWriter = CSVWriter.open(outputStream)
    val headers = results.head.getSchema.getAttributeNames
    csvWriter.writeRow(headers)
    results.foreach { tuple =>
      csvWriter.writeRow(tuple.getFields.toIndexedSeq)
    }
    csvWriter.close()
  }

  private def writeArrow(outputStream: OutputStream, results: Iterable[Tuple]): Unit = {
    if (results.isEmpty) return

    val allocator = new RootAllocator()
    Using.Manager { use =>
      val (writer, root) = createArrowWriter(results, allocator, outputStream)
      use(writer)
      use(root)
      use(allocator)

      writer.start()
      val batchSize = 1000
      val resultList = results.toList
      val totalSize = resultList.size

      for (batchStart <- 0 until totalSize by batchSize) {
        val batchEnd = Math.min(batchStart + batchSize, totalSize)
        val currentBatchSize = batchEnd - batchStart
        for (i <- 0 until currentBatchSize) {
          val tuple = resultList(batchStart + i)
          ArrowUtils.setTexeraTuple(tuple, i, root)
        }
        root.setRowCount(currentBatchSize)
        writer.writeBatch()
        root.clear()
      }
      writer.end()
    }
  }

  /**
    * Export multiple operators' results as a single ZIP file stream.
    */
  def exportOperatorsAsZip(
      user: User,
      request: ResultExportRequest
  ): (StreamingOutput, Option[String]) = {
    if (request.operatorIds.isEmpty) {
      return (null, None)
    }

    val timestamp = LocalDateTime
      .now()
      .truncatedTo(ChronoUnit.SECONDS)
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))
    val zipFileName = s"${request.workflowName}-$timestamp.zip"

    val streamingOutput: StreamingOutput = new StreamingOutput {
      override def write(outputStream: OutputStream): Unit = {
        Using.resource(new ZipOutputStream(outputStream)) { zipOut =>
          request.operatorIds.foreach { opId =>
            val execIdOpt = getLatestExecutionId(workflowIdentity)
            if (execIdOpt.isEmpty) {
              throw new WebApplicationException(
                s"No execution result for workflow ${request.workflowId}"
              )
            }

            val operatorResult = generateOneOperatorResult(opId)
            if (operatorResult == null || operatorResult.getCount == 0) {
              // create an "empty" file for this operator
              zipOut.putNextEntry(new ZipEntry(s"$opId-empty.txt"))
              val msg = s"Operator $opId has no results"
              zipOut.write(msg.getBytes(StandardCharsets.UTF_8))
              zipOut.closeEntry()
            } else {
              val results = operatorResult.get().to(Iterable)
              val extension = request.exportType match {
                case "csv"   => "csv"
                case "arrow" => "arrow"
                case "data"  => "bin"
                case _       => "dat"
              }
              val operatorFileName = generateFileName(request, opId, extension)

              zipOut.putNextEntry(new ZipEntry(operatorFileName))
              val nonClosingStream = new NonClosingOutputStream(zipOut)

              request.exportType match {
                case "csv"   => writeCsv(nonClosingStream, results)
                case "arrow" => writeArrow(nonClosingStream, results)
                case "data" =>
                  writeData(nonClosingStream, request, results) // handle single cell export
                case _ => writeCsv(nonClosingStream, results)
              }
              zipOut.closeEntry()
            }
          }
        }
      }
    }

    (streamingOutput, Some(zipFileName))
  }
}
