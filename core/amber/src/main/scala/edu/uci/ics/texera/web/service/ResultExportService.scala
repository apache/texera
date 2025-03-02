package edu.uci.ics.texera.web.service

import com.github.tototoshi.csv.CSVWriter
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.util.PathUtils
import edu.uci.ics.amber.core.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.model.websocket.request.ResultExportRequest
import edu.uci.ics.texera.web.model.websocket.response.ResultExportResponse
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.createNewDatasetVersionByAddingFiles
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.{
  WorkflowExecutionsResource,
  WorkflowVersionResource
}
import edu.uci.ics.amber.util.ArrowUtils
import edu.uci.ics.texera.web.service.WorkflowExecutionService.getLatestExecutionId
import java.io.{PipedInputStream, PipedOutputStream}
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.{Executors, ThreadPoolExecutor}
import scala.collection.mutable
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.commons.lang3.StringUtils
import java.io.OutputStream
import java.nio.channels.Channels
import scala.util.Using
import edu.uci.ics.amber.core.workflow.PortIdentity

object ResultExportService {
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
    // Only supports external port 0 for now. TODO: support multiple ports
    val storageUri = WorkflowExecutionsResource.getResultUriByExecutionAndPort(
      workflowIdentity,
      getLatestExecutionId(workflowIdentity).get,
      OperatorIdentity(request.operatorId),
      None,
      PortIdentity()
    )
    val operatorResult: VirtualDocument[Tuple] =
      DocumentFactory.openDocument(storageUri.get)._1.asInstanceOf[VirtualDocument[Tuple]]
    if (operatorResult.getCount == 0) {
      return ResultExportResponse("error", "The workflow contains no results")
    }

    val results: Iterable[Tuple] = operatorResult.get().to(Iterable)
    val attributeNames = results.head.getSchema.getAttributeNames

    // handle the request according to export type
    request.exportType match {
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

  private def handleDataRequest(
      user: User,
      request: ResultExportRequest,
      results: Iterable[Tuple]
  ): ResultExportResponse = {
    val rowIndex = request.rowIndex
    val columnIndex = request.columnIndex
    val filename = request.filename

    if (rowIndex >= results.size || columnIndex >= results.head.getFields.length) {
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
      WorkflowVersionResource.getLatestVersion(Integer.valueOf(request.workflowId))
    val timestamp = LocalDateTime
      .now()
      .truncatedTo(ChronoUnit.SECONDS)
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))
    StringUtils.replaceEach(
      s"${request.workflowName}-v$latestVersion-${request.operatorName}-$timestamp.$extension",
      Array("/", "\\"),
      Array("", "")
    )
  }

  private def saveToDatasets(
      request: ResultExportRequest,
      user: User,
      pipedInputStream: PipedInputStream,
      fileName: String
  ): Unit = {
    request.datasetIds.foreach { did =>
      val datasetPath = PathUtils.getDatasetPath(Integer.valueOf(did))
      val filePath = datasetPath.resolve(fileName)
      createNewDatasetVersionByAddingFiles(
        Integer.valueOf(did),
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
