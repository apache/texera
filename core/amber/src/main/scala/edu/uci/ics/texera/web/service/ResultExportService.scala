package edu.uci.ics.texera.web.service

import com.github.tototoshi.csv.CSVWriter
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.drive.Drive
import com.google.api.services.drive.model.{File, FileList, Permission}
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model.{Spreadsheet, SpreadsheetProperties, ValueRange}
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.engine.common.Utils.retry
import edu.uci.ics.amber.util.PathUtils
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.request.ResultExportRequest
import edu.uci.ics.texera.web.model.websocket.response.ResultExportResponse
import edu.uci.ics.texera.web.resource.GoogleResource
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.createNewDatasetVersionByAddingFiles
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.{WorkflowExecutionsResource, WorkflowVersionResource}
import org.jooq.types.UInteger
import edu.uci.ics.amber.util.ArrowUtils
import edu.uci.ics.texera.web.service.WorkflowExecutionService.getLatestExecutionId

import java.io.{PipedInputStream, PipedOutputStream}
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util
import java.util.concurrent.{Executors, ThreadPoolExecutor}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.SeqHasAsJava
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.commons.lang3.StringUtils

import java.io.OutputStream
import java.nio.channels.Channels
import java.util.zip.{ZipEntry, ZipOutputStream}
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.StreamingOutput
import scala.util.Using
import java.io.{FilterOutputStream, IOException, OutputStream}

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
  final private val UPLOAD_BATCH_ROW_COUNT = 10000
  final private val RETRY_ATTEMPTS = 7
  final private val BASE_BACK_OOF_TIME_IN_MS = 1000
  final private val WORKFLOW_RESULT_FOLDER_NAME = "workflow_results"
  final private val pool: ThreadPoolExecutor =
    Executors.newFixedThreadPool(3).asInstanceOf[ThreadPoolExecutor]
}

class ResultExportService(workflowIdentity: WorkflowIdentity) {

  import ResultExportService._

  private val cache = new mutable.HashMap[String, String]

  private def generateOneOperatorResult(operatorId: String): VirtualDocument[Tuple] = {
    // By now the workflow should finish running
    // Only supports external port 0 for now. TODO: support multiple ports
    val storageUri = WorkflowExecutionsResource.getResultUriByExecutionAndPort(
      workflowIdentity,
      getLatestExecutionId(workflowIdentity).get,
      OperatorIdentity(operatorId),
      PortIdentity()
    )
    /* Fix the error, I want to return an empty VirtualDocument so caller of this function can check its empty by .getCount() */
    storageUri.map(uri => DocumentFactory.openDocument(uri)._1.asInstanceOf[VirtualDocument[Tuple]]).orNull
  }

  def exportResult(user: User, request: ResultExportRequest): ResultExportResponse = {
    val successMessages = new mutable.ListBuffer[String]()
    val errorMessages = new mutable.ListBuffer[String]()

    // iterate through all operator IDs
    request.operatorIds.foreach { opId =>
      try {
        val (messageOpt, errorOpt) = exportSingleOperator(user, request, opId)
        messageOpt.foreach(successMessages += _)
        errorOpt.foreach(errorMessages += _)
      } catch {
        case ex: Exception =>
          // catch any unforeseen exceptions so that other operators can still be attempted
          errorMessages += s"Error exporting operator $opId: ${ex.getMessage}"
      }
    }

    if (errorMessages.isEmpty) {
      ResultExportResponse("success", successMessages.mkString("\n"))
    } else if (successMessages.isEmpty) {
      ResultExportResponse("error", errorMessages.mkString("\n"))
    } else {
      ResultExportResponse("success", successMessages.mkString("\n"))
      // partial success
      // TODO: user should be informed that some of the operators are not exported due to errors,
      //       currently, we assume the errors are due to no result so we just inform the user
      //       that request was successful since at least one operator was exported successfully
//      ResultExportResponse(
//        "partial",
//        s"Some operators succeeded:\n${successMessages.mkString("\n")}\n\n" +
//          s"Some operators failed:\n${errorMessages.mkString("\n")}"
//      )
    }
  }

  /**
   * Export the result for ONE operator.
   * Return (SomeSuccessMessage, SomeErrorMessage) or (None, None) if handled differently
   */
  private def exportSingleOperator(
                                    user: User,
                                    request: ResultExportRequest,
                                    operatorId: String
                                  ): (Option[String], Option[String]) = {

    // Possibly use some caching key
    val cacheKey = s"${request.exportType}-$operatorId"
    if (cache.contains(cacheKey)) {
      return (
        Some(s"Link retrieved from cache for operator $operatorId: ${cache(cacheKey)}"),
        None
      )
    }

    val execIdOpt = getLatestExecutionId(workflowIdentity)
    if (execIdOpt.isEmpty) {
      return (None, Some(s"Workflow ${request.workflowId} has no execution result"))
    }

    val operatorResult = generateOneOperatorResult(operatorId)
    if (operatorResult == null || operatorResult.getCount == 0) {
      return (Option("error"), Option("The workflow contains no results"))
    }

    val results: Iterable[Tuple] = operatorResult.get().to(Iterable)
    val attributeNames = results.head.getSchema.getAttributeNames

    request.exportType match {
      case "google_sheet" =>
        val (msg, err) = handleGoogleSheetRequest(operatorId, results, attributeNames, request)
        (msg, err)

      case "csv" =>
        val (msg, err) = handleCSVRequest(operatorId, user, request, results, attributeNames)
        (msg, err)

      case "data" =>
        val (msg, err) = handleDataRequest(operatorId, user, request, results)
        (msg, err)

      case "arrow" =>
        val (msg, err) = handleArrowRequest(operatorId, user, request, results)
        (msg, err)

      case unknown =>
        (None, Some(s"Unknown export type: $unknown"))
    }
  }

  private def handleCSVRequest(
                                operatorId: String,
                                user: User,
                                request: ResultExportRequest,
                                results: Iterable[Tuple],
                                headers: List[String]
                              ): (Option[String], Option[String]) = {
    try {
      val pipedOutputStream = new PipedOutputStream()
      val pipedInputStream = new PipedInputStream(pipedOutputStream)

      pool.submit(new Runnable {
        override def run(): Unit = {
          val writer = CSVWriter.open(pipedOutputStream)
          writer.writeRow(headers)
          results.foreach { tuple =>
            writer.writeRow(tuple.getFields.toIndexedSeq)
          }
          writer.close()
        }
      })

      val fileName = generateFileName(request, operatorId, "csv")
      saveToDatasets(request, user, pipedInputStream, fileName)
      (Some(s"CSV export done for operator $operatorId -> file: $fileName"), None)
    } catch {
      case ex: Exception =>
        (None, Some(s"CSV export failed for operator $operatorId: ${ex.getMessage}"))
    }
  }

  private def handleGoogleSheetRequest(
                                        operatorId: String,
                                        results: Iterable[Tuple],
                                        header: List[String],
                                        request: ResultExportRequest
                                      ): (Option[String], Option[String]) = {
    try {
      val sheetService: Sheets = GoogleResource.getSheetService
      val sheetId: String = createGoogleSheet(sheetService, s"${request.workflowName}-$operatorId")
      if (sheetId == null) {
        return (None, Some(s"Fail to create google sheet for operator $operatorId"))
      }

      val driveService: Drive = GoogleResource.getDriveService
      moveToResultFolder(driveService, sheetId)

      // share
      val perm = new Permission().setType("anyone").setRole("reader")
      driveService.permissions().create(sheetId, perm).execute()

      // asynchronously upload data
      pool.submit(new Runnable {
        override def run(): Unit = {
          uploadHeader(sheetService, sheetId, header)
          uploadResult(sheetService, sheetId, results)
        }
      })

      val link = s"https://docs.google.com/spreadsheets/d/$sheetId/edit"
      // you can store in a small local cache if you want
      val cacheKey = s"${request.exportType}-$operatorId"
      cache(cacheKey) = link

      val msg = s"Google sheet created for operator $operatorId: $link (results are uploading)"
      (Some(msg), None)
    } catch {
      case ex: Exception =>
        (None, Some(s"Google Sheet export failed for operator $operatorId: ${ex.getMessage}"))
    }
  }

  private def createGoogleSheet(sheetService: Sheets, title: String): String = {
    val sheetProps = new SpreadsheetProperties().setTitle(title)
    val createReq = new Spreadsheet().setProperties(sheetProps)
    val target = sheetService.spreadsheets.create(createReq).setFields("spreadsheetId").execute()
    target.getSpreadsheetId
  }

  @tailrec
  private def moveToResultFolder(
                                  driveService: Drive,
                                  sheetId: String,
                                  retryOnce: Boolean = true
                                ): Unit = {
    val folderId = retrieveResultFolderId(driveService)
    try {
      driveService.files().update(sheetId, null).setAddParents(folderId).execute()
    } catch {
      case ex: GoogleJsonResponseException =>
        if (retryOnce) {
          // maybe folder was deleted/trash, so try again
          moveToResultFolder(driveService, sheetId, retryOnce = false)
        } else {
          throw ex
        }
    }
  }

  private def retrieveResultFolderId(driveService: Drive): String =
    synchronized {
      val folderResult: FileList =
        driveService
          .files()
          .list()
          .setQ(
            s"mimeType = 'application/vnd.google-apps.folder' and name='$WORKFLOW_RESULT_FOLDER_NAME'"
          )
          .setSpaces("drive")
          .execute()

      if (folderResult.getFiles.isEmpty) {
        val fileMetadata = new File()
        fileMetadata.setName(WORKFLOW_RESULT_FOLDER_NAME)
        fileMetadata.setMimeType("application/vnd.google-apps.folder")
        val targetFolder: File = driveService.files.create(fileMetadata).setFields("id").execute()
        targetFolder.getId
      } else {
        folderResult.getFiles.get(0).getId
      }
    }

  private def uploadHeader(sheetService: Sheets, sheetId: String, header: List[AnyRef]): Unit = {
    uploadContent(sheetService, sheetId, List(header.asJava).asJava)
  }

  private def uploadResult(sheetService: Sheets, sheetId: String, result: Iterable[Tuple]): Unit = {
    val batch = new util.ArrayList[util.List[AnyRef]](UPLOAD_BATCH_ROW_COUNT)

    for (tuple <- result) {
      val row: util.List[AnyRef] = tuple.getFields.map(convertUnsupported).toList.asJava
      batch.add(row)

      if (batch.size() == UPLOAD_BATCH_ROW_COUNT) {
        uploadContent(sheetService, sheetId, batch)
        batch.clear()
      }
    }
    if (!batch.isEmpty) {
      uploadContent(sheetService, sheetId, batch)
    }
  }

  private def uploadContent(
                             sheetService: Sheets,
                             sheetId: String,
                             content: util.List[util.List[AnyRef]]
                           ): Unit = {
    val body = new ValueRange().setValues(content)
    val range = "A1"
    val options = "RAW"
    retry(attempts = RETRY_ATTEMPTS, baseBackoffTimeInMS = BASE_BACK_OOF_TIME_IN_MS) {
      sheetService.spreadsheets
        .values()
        .append(sheetId, range, body)
        .setValueInputOption(options)
        .execute()
    }
  }

  private def convertUnsupported(anyVal: Any): AnyRef = {
    anyVal match {
      case null      => ""
      case s: String => s
      case n: Number => n
      case other     => other.toString
    }
  }

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

      val pipedOutputStream = new PipedOutputStream()
      val pipedInputStream = new PipedInputStream(pipedOutputStream)

      pool.submit(new Runnable {
        override def run(): Unit = {
          pipedOutputStream.write(dataBytes)
          pipedOutputStream.close()
        }
      })

      saveToDatasets(request, user, pipedInputStream, fileName)
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
      val pipedOutputStream = new PipedOutputStream()
      val pipedInputStream = new PipedInputStream(pipedOutputStream)
      val allocator = new RootAllocator()

      pool.submit(() => {
        Using.Manager { use =>
          val (writer, root) = createArrowWriter(results, allocator, pipedOutputStream)
          use(writer)
          use(root)
          use(allocator)
          use(pipedOutputStream)

          writeArrowData(writer, root, results)
        }
      })

      val fileName = generateFileName(request, operatorId, "arrow")
      saveToDatasets(request, user, pipedInputStream, fileName)

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

  private def generateFileName(
                                request: ResultExportRequest,
                                operatorId: String,
                                extension: String
                              ): String = {
    val latestVersion =
      WorkflowVersionResource.getLatestVersion(org.jooq.types.UInteger.valueOf(request.workflowId))
    val timestamp = LocalDateTime
      .now()
      .truncatedTo(ChronoUnit.SECONDS)
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))

    val rawName = s"${request.workflowName}-op$operatorId-v$latestVersion-$timestamp.$extension"
    // remove any path separators
    StringUtils.replaceEach(rawName, Array("/", "\\"), Array("", ""))
  }

  private def saveToDatasets(
                              request: ResultExportRequest,
                              user: User,
                              pipedInputStream: PipedInputStream,
                              fileName: String
                            ): Unit = {
    request.datasetIds.foreach { did =>
      val datasetPath = PathUtils.getDatasetPath(org.jooq.types.UInteger.valueOf(did))
      val filePath = datasetPath.resolve(fileName)
      createNewDatasetVersionByAddingFiles(
        org.jooq.types.UInteger.valueOf(did),
        user,
        Map(filePath -> pipedInputStream)
      )
    }
  }

  /**
   * For local download of a single operator. Streams the data directly.
   * We return (StreamingOutput, Some(filename)) on success, or (null, None) on error.
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
      case other   => "dat"
    }

    val fileName = generateFileName(request, operatorId, extension)

    val streamingOutput: StreamingOutput = new StreamingOutput {
      override def write(out: OutputStream): Unit = {
        request.exportType match {
          case "csv"   => writeCsv(out, results)
          case "arrow" => writeArrow(out, results)
          case _       => writeCsv(out, results) // fallback
        }
      }
    }

    (streamingOutput, Some(fileName))
  }

  /**
   * Writes CSV to output stream
   */
  private def writeCsv(outputStream: OutputStream, results: Iterable[Tuple]): Unit = {
    // for large data, you might want a buffered approach
    val csvWriter = CSVWriter.open(outputStream) // Tototoshi CSVWriter can open an OutputStream
    val headers = results.head.getSchema.getAttributeNames
    csvWriter.writeRow(headers)
    results.foreach { tuple =>
      csvWriter.writeRow(tuple.getFields.toIndexedSeq)
    }
    csvWriter.close()
  }

  /**
   * Writes Arrow to output stream
   */
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

    val streamingOutput = new StreamingOutput {
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
              // create empty record
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

              // create a non-closing wrapper around zipOut
              val nonClosingStream = new NonClosingOutputStream(zipOut)

              request.exportType match {
                case "csv"   => writeCsv(nonClosingStream, results)
                case "arrow" => writeArrow(nonClosingStream, results)
                case _       => writeCsv(nonClosingStream, results)
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
