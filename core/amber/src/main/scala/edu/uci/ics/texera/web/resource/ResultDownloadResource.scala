package edu.uci.ics.texera.web.resource

import com.google.api.client.util.Lists
import com.google.api.services.drive.Drive
import com.google.api.services.drive.model.Permission
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model.{
  AppendValuesResponse,
  Spreadsheet,
  SpreadsheetProperties,
  ValueRange
}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.model.event.ResultDownloadResponse
import edu.uci.ics.texera.web.model.request.ResultDownloadRequest
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource.{
  sessionDownloadCache,
  sessionResults
}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import java.util
import java.util.stream.Collectors
import scala.collection.JavaConverters._

import scala.collection.mutable

object ResultDownloadResource {

  private final val UPLOAD_SIZE = 100;

  def apply(
      sessionId: String,
      request: ResultDownloadRequest
  ): ResultDownloadResponse = {
    // retrieve the file link saved in the session if exists
    if (
      sessionDownloadCache.contains(sessionId) && sessionDownloadCache(sessionId).contains(
        request.downloadType
      )
    ) {
      return ResultDownloadResponse(
        request.downloadType,
        sessionDownloadCache(sessionId)(request.downloadType),
        "File retrieved from cache."
      )
    }

    // By now the workflow should finish running. Only one operator should contain results
    // TODO currently assume only one operator should contains the result
    // TODO change status checking of the workflow
    val OperatorWithResult = sessionResults(sessionId).count(p => p._2.nonEmpty)
    if (OperatorWithResult == 0) {
      return ResultDownloadResponse(
        request.downloadType,
        "",
        "The workflow contains no results"
      )
    } else if (OperatorWithResult > 1) {
      // more than one operator contains results means the workflow does not finish running.
      return ResultDownloadResponse(
        request.downloadType,
        "",
        "The workflow does not finish running"
      )
    }

    // convert the ITuple into tuple
    // TODO currently only accept the tuple as input
    val results: List[Tuple] =
      sessionResults(sessionId).values
        .find(p => p.nonEmpty)
        .get
        .map(ituple => ituple.asInstanceOf[Tuple])
    val schema = getSchema(results.head)

    // handle the request according to download type
    var response: ResultDownloadResponse = null
    request.downloadType match {
      case "google_sheet" =>
        response = handleGoogleSheetRequest(request, results, schema)
      case _ =>
        response = ResultDownloadResponse(
          request.downloadType,
          "",
          s"Unknown download type: ${request.downloadType}"
        )
    }

    // save the file link in the session cache
    if (!sessionDownloadCache.contains(sessionId)) {
      sessionDownloadCache.put(
        sessionId,
        mutable.HashMap(request.downloadType -> response.link)
      )
    } else {
      sessionDownloadCache(sessionId)
        .put(request.downloadType, response.link)
    }

    response
  }

  // get the schema from the sample tuple
  private def getSchema(tuple: Tuple): util.List[AnyRef] = {
    tuple.getSchema.getAttributeNames
      .asInstanceOf[util.List[AnyRef]]
  }

  private def handleGoogleSheetRequest(
      resultDownloadRequest: ResultDownloadRequest,
      result: List[ITuple],
      schema: util.List[AnyRef]
  ): ResultDownloadResponse = {
    // create google sheet
    val sheetService: Sheets = GoogleResource.getSheetService
    val sheetId: String =
      createGoogleSheet(sheetService, resultDownloadRequest.workflowName)
    if (sheetId == null)
      return ResultDownloadResponse(
        resultDownloadRequest.downloadType,
        "",
        "Fail to create google sheet"
      )

    // upload the schema
    val schemaContent: util.List[util.List[AnyRef]] = Lists.newArrayList()
    schemaContent.add(schema)
    val response: AppendValuesResponse =
      uploadContent(sheetService, sheetId, schemaContent)

    // allow user to access this sheet in the service account
    val drive: Drive = GoogleResource.getDriveService
    val sharePermission: Permission = new Permission()
      .setType("anyone")
      .setRole("reader")
    drive
      .permissions()
      .create(sheetId, sharePermission)
      .execute()

    // upload the content asynchronously to avoid long waiting on the user side.
    // may change to thread pool
    new Thread(() => uploadResult(sheetService, sheetId, result)).start()

    // generate success response
    val link: String = s"https://docs.google.com/spreadsheets/d/$sheetId/edit"
    val message: String =
      s"Google sheet created. The results may be still uploading."
    ResultDownloadResponse(resultDownloadRequest.downloadType, link, message)
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

  /**
    * upload the result body to the google sheet
    */
  private def uploadResult(sheetService: Sheets, sheetId: String, result: List[ITuple]): Unit = {
    val content: util.List[util.List[AnyRef]] =
      Lists.newArrayListWithCapacity(UPLOAD_SIZE)
    // use for loop to avoid copying the whole result at the same time
    for (tuple: ITuple <- result) {
      val tupleContent: util.List[AnyRef] =
        tuple
          .asInstanceOf[Tuple]
          .getFields
          .stream()
          .map(tuple => convertUnsupported(tuple))
          .toArray
          .toList
          .asJava
      content.add(tupleContent)

      if (content.size() == UPLOAD_SIZE) {
        // TODO the response is from uploading is not checked.
        //  The design for the response seems not to be designed for error handling
        // it will throw error and stop if encounter error during uploading
        val response: AppendValuesResponse =
          uploadContent(sheetService, sheetId, content)
        content.clear()
      }
    }

    if (!content.isEmpty) {
      val response: AppendValuesResponse =
        uploadContent(sheetService, sheetId, content)
    }
  }

  /**
    * convert the tuple content into the type the Google Sheet API supports
    */
  private def convertUnsupported(content: AnyRef): AnyRef = {
    // Google Sheet API supports String and number(long, int, double and so on)
    if (content.isInstanceOf[String] || content.isInstanceOf[Number]) {
      return content
    }
    // convert all the other type into String
    content.toString
  }

  /**
    * upload the content to the google sheet
    * The type of content is java list because the google API is in java
    */
  private def uploadContent(
      sheetService: Sheets,
      sheetId: String,
      content: util.List[util.List[AnyRef]]
  ): AppendValuesResponse = {
    val body: ValueRange = new ValueRange().setValues(content)
    val range: String = "A1"
    val valueInputOption: String = "RAW"
    sheetService.spreadsheets.values
      .append(sheetId, range, body)
      .setValueInputOption(valueInputOption)
      .execute
  }
}
