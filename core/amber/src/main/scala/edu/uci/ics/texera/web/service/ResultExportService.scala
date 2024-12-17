package edu.uci.ics.texera.web.service

import com.github.tototoshi.csv.CSVWriter
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.util.Lists
import com.google.api.services.drive.Drive
import com.google.api.services.drive.model.{File, FileList, Permission}
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model.{Spreadsheet, SpreadsheetProperties, ValueRange}
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.storage.result.{OpResultStorage, ResultStorage}
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.engine.common.Utils.retry
import edu.uci.ics.amber.util.PathUtils
import edu.uci.ics.amber.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.request.ResultExportRequest
import edu.uci.ics.texera.web.model.websocket.response.ResultExportResponse
import edu.uci.ics.texera.web.resource.GoogleResource
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.{
  createNewDatasetVersionByAddingFiles,
  sanitizePath
}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowVersionResource
import org.jooq.types.UInteger
import edu.uci.ics.amber.util.ArrowUtils

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

import java.io.OutputStream
import java.nio.channels.Channels
import scala.util.Using

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

  def exportResult(
      user: User,
      request: ResultExportRequest
  ): ResultExportResponse = {
    // retrieve the file link saved in the session if exists
    if (cache.contains(request.exportType)) {
      return ResultExportResponse(
        "success",
        s"Link retrieved from cache ${cache(request.exportType)}"
      )
    }

    // By now the workflow should finish running
    val operatorResult: VirtualDocument[Tuple] =
      ResultStorage.getOpResultStorage(workflowIdentity).get(OperatorIdentity(request.operatorId))
    if (operatorResult == null) {
      return ResultExportResponse("error", "The workflow contains no results")
    }

    val results: Iterable[Tuple] = operatorResult.get().to(Iterable)
    val attributeNames = results.head.getSchema.getAttributeNames

    // handle the request according to export type
    request.exportType match {
      case "google_sheet" =>
        handleGoogleSheetRequest(cache, request, results, attributeNames)
      case "csv" =>
        handleCSVRequest(user, request, results, attributeNames)
      case "data" =>
        handleDataRequest(user, request, results)
      case "arrow" =>
        handleArrowRequest(user, request, results)
      case _ =>
        ResultExportResponse("error", s"Unknown export type: ${request.exportType}")
    }
  }

  private def handleCSVRequest(
      user: User,
      request: ResultExportRequest,
      results: Iterable[Tuple],
      headers: List[String]
  ): ResultExportResponse = {
    val pipedOutputStream = new PipedOutputStream()
    val pipedInputStream = new PipedInputStream(pipedOutputStream)

    pool.submit(() =>
      {
        val writer = CSVWriter.open(pipedOutputStream)
        writer.writeRow(headers)
        results.foreach { tuple =>
          writer.writeRow(tuple.getFields.toIndexedSeq)
        }
        writer.close()
      }.asInstanceOf[Runnable]
    )

    val fileName = generateFileName(request, "csv")
    saveToDatasets(request, user, pipedInputStream, fileName)

    ResultExportResponse(
      "success",
      s"File saved to User Dashboard as $fileName to Datasets ${request.datasetIds.mkString(",")}"
    )
  }

  private def handleGoogleSheetRequest(
      exportCache: mutable.HashMap[String, String],
      request: ResultExportRequest,
      results: Iterable[Tuple],
      header: List[String]
  ): ResultExportResponse = {
    // create google sheet
    val sheetService: Sheets = GoogleResource.getSheetService
    val sheetId: String =
      createGoogleSheet(sheetService, request.workflowName)
    if (sheetId == null) {
      return ResultExportResponse("error", "Fail to create google sheet")
    }

    val driveService: Drive = GoogleResource.getDriveService
    moveToResultFolder(driveService, sheetId)

    // allow user to access this sheet in the service account
    val sharePermission: Permission = new Permission()
      .setType("anyone")
      .setRole("reader")
    driveService
      .permissions()
      .create(sheetId, sharePermission)
      .execute()

    // upload the content asynchronously to avoid long waiting on the user side.
    pool
      .submit(() =>
        {
          uploadHeader(sheetService, sheetId, header)
          uploadResult(sheetService, sheetId, results)
        }.asInstanceOf[Runnable]
      )

    // generate success response
    val link = s"https://docs.google.com/spreadsheets/d/$sheetId/edit"
    val message: String =
      s"Google sheet created. The results may be still uploading. You can access the sheet $link"
    // save the file link in the session cache
    exportCache(request.exportType) = link
    ResultExportResponse("success", message)
  }

  /**
    * create the google sheet and return the sheet Id
    */
  private def createGoogleSheet(sheetService: Sheets, workflowName: String): String = {
    val createSheetRequest = new Spreadsheet()
      .setProperties(new SpreadsheetProperties().setTitle(workflowName))
    val targetSheet: Spreadsheet = sheetService.spreadsheets
      .create(createSheetRequest)
      .setFields("spreadsheetId")
      .execute
    targetSheet.getSpreadsheetId
  }

  private def handleDataRequest(
      user: User,
      request: ResultExportRequest,
      results: Iterable[Tuple]
  ): ResultExportResponse = {
    val rowIndex = request.rowIndex
    val columnIndex = request.columnIndex
    val filename = request.filename

    if (rowIndex >= results.size || columnIndex >= results.head.getFields.size) {
      return ResultExportResponse("error", s"Invalid row or column index")
    }

    val selectedRow = results.toSeq(rowIndex)
    val field: Any = selectedRow.getField(columnIndex)
    val dataBytes: Array[Byte] = convertFieldToBytes(field)

    val pipedOutputStream = new PipedOutputStream()
    val pipedInputStream = new PipedInputStream(pipedOutputStream)

    pool.submit(() =>
      {
        pipedOutputStream.write(dataBytes)
        pipedOutputStream.close()
      }.asInstanceOf[Runnable]
    )

    saveToDatasets(request, user, pipedInputStream, filename)

    ResultExportResponse(
      "success",
      s"Data file $filename saved to Datasets ${request.datasetIds.mkString(",")}"
    )
  }

  /**
    * move the workflow results to a specific folder
    */
  @tailrec
  private def moveToResultFolder(
      driveService: Drive,
      sheetId: String,
      retry: Boolean = true
  ): Unit = {
    val folderId = retrieveResultFolderId(driveService)
    try {
      driveService
        .files()
        .update(sheetId, null)
        .setAddParents(folderId)
        .execute()
    } catch {
      case exception: GoogleJsonResponseException =>
        if (retry) {
          // This exception maybe caused by the full deletion of the target folder and
          // the cached folder id is obsolete.
          //  * note: by full deletion, the folder has to be deleted from trash as well.
          // In this case, try again.
          moveToResultFolder(driveService, sheetId, retry = false)
        } else {
          // if the exception continues to show up then just throw it normally.
          throw exception
        }
    }
  }

  private def retrieveResultFolderId(driveService: Drive): String =
    synchronized {
      val folderResult: FileList = driveService
        .files()
        .list()
        .setQ(
          s"mimeType = 'application/vnd.google-apps.folder' and name='$WORKFLOW_RESULT_FOLDER_NAME'"
        )
        .setSpaces("drive")
        .execute()

      if (folderResult.getFiles.isEmpty) {
        val fileMetadata: File = new File()
        fileMetadata.setName(WORKFLOW_RESULT_FOLDER_NAME)
        fileMetadata.setMimeType("application/vnd.google-apps.folder")
        val targetFolder: File = driveService.files.create(fileMetadata).setFields("id").execute
        targetFolder.getId
      } else {
        folderResult.getFiles.get(0).getId
      }
    }

  /**
    * upload the result header to the google sheet
    */
  private def uploadHeader(
      sheetService: Sheets,
      sheetId: String,
      header: List[AnyRef]
  ): Unit = {
    uploadContent(sheetService, sheetId, List(header.asJava).asJava)
  }

  /**
    * upload the result body to the google sheet
    */
  private def uploadResult(
      sheetService: Sheets,
      sheetId: String,
      result: Iterable[Tuple]
  ): Unit = {
    val content: util.List[util.List[AnyRef]] =
      Lists.newArrayListWithCapacity(UPLOAD_BATCH_ROW_COUNT)
    // use for loop to avoid copying the whole result at the same time
    for (tuple: Tuple <- result) {

      val tupleContent: util.List[AnyRef] =
        tuple.getFields
          .map(convertUnsupported)
          .toArray
          .toList
          .asJava
      content.add(tupleContent)

      if (content.size() == UPLOAD_BATCH_ROW_COUNT) {
        uploadContent(sheetService, sheetId, content)
        content.clear()
      }
    }

    if (!content.isEmpty) {
      uploadContent(sheetService, sheetId, content)
    }
  }

  /**
    * convert the tuple content into the type the Google Sheet API supports
    */
  private def convertUnsupported(content: Any): AnyRef = {
    content match {

      // if null, use empty string to represent.
      case null => ""

      // Google Sheet API supports String and number(long, int, double and so on)
      case _: String | _: Number => content.asInstanceOf[AnyRef]

      // convert all the other type into String
      case _ => content.toString
    }

  }

  /**
    * upload the content to the google sheet
    * The type of content is java list because the google API is in java
    */
  private def uploadContent(
      sheetService: Sheets,
      sheetId: String,
      content: util.List[util.List[AnyRef]]
  ): Unit = {
    val body: ValueRange = new ValueRange().setValues(content)
    val range: String = "A1"
    val valueInputOption: String = "RAW"

    // using retry logic here, to handle possible API errors, i.e., rate limit exceeded.
    retry(attempts = RETRY_ATTEMPTS, baseBackoffTimeInMS = BASE_BACK_OOF_TIME_IN_MS) {
      sheetService.spreadsheets.values
        .append(sheetId, range, body)
        .setValueInputOption(valueInputOption)
        .execute
    }

  }

  private def handleArrowRequest(
      user: User,
      request: ResultExportRequest,
      results: Iterable[Tuple]
  ): ResultExportResponse = {
    if (results.isEmpty) {
      return ResultExportResponse("error", "No results to export")
    }

    val pipedOutputStream = new PipedOutputStream()
    val pipedInputStream = new PipedInputStream(pipedOutputStream)
    val allocator = new RootAllocator()

    pool.submit(() =>
      {
        Using.Manager { use =>
          val (writer, root) = createArrowWriter(results, allocator, pipedOutputStream)
          use(writer)
          use(root)
          use(allocator)
          use(pipedOutputStream)

          writeArrowData(writer, root, results)
        }
      }.asInstanceOf[Runnable]
    )

    val fileName = generateFileName(request, "arrow")
    saveToDatasets(request, user, pipedInputStream, fileName)

    ResultExportResponse(
      "success",
      s"Arrow file saved as $fileName to Datasets ${request.datasetIds.mkString(",")}"
    )
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

    // Convert to Seq to get total size
    val resultSeq = results.toSeq
    val totalSize = resultSeq.size

    // Process in complete batches
    for (batchStart <- 0 until totalSize by batchSize) {
      val batchEnd = Math.min(batchStart + batchSize, totalSize)
      val currentBatchSize = batchEnd - batchStart

      // Process each tuple in the current batch
      for (i <- 0 until currentBatchSize) {
        val tuple = resultSeq(batchStart + i)
        ArrowUtils.setTexeraTuple(tuple, i, root)
      }

      // Set the correct row count for this batch and write it
      root.setRowCount(currentBatchSize)
      writer.writeBatch()
      root.clear()
    }

    writer.end()
  }

  private def generateFileName(request: ResultExportRequest, extension: String): String = {
    val latestVersion =
      WorkflowVersionResource.getLatestVersion(UInteger.valueOf(request.workflowId))
    val timestamp = LocalDateTime
      .now()
      .truncatedTo(ChronoUnit.SECONDS)
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))
    sanitizePath(
      s"${request.workflowName}-v$latestVersion-${request.operatorName}-$timestamp.$extension"
    )
  }

  private def saveToDatasets(
      request: ResultExportRequest,
      user: User,
      pipedInputStream: PipedInputStream,
      fileName: String
  ): Unit = {
    request.datasetIds.foreach { did =>
      val datasetPath = PathUtils.getDatasetPath(UInteger.valueOf(did))
      val filePath = datasetPath.resolve(fileName)
      createNewDatasetVersionByAddingFiles(
        UInteger.valueOf(did),
        user,
        Map(filePath -> pipedInputStream)
      )
    }
  }

  private def convertFieldToBytes(field: Any): Array[Byte] = {
    field match {
      case data: Array[Byte] => data
      case data: String      => data.getBytes(StandardCharsets.UTF_8)
      case data              => data.toString.getBytes(StandardCharsets.UTF_8)
    }
  }
}
