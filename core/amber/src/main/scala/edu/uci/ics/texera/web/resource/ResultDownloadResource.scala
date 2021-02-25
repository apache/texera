package edu.uci.ics.texera.web.resource

import java.util

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
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import javax.websocket.Session

object ResultDownloadResource {

  private final val SESSION_CACHED_RESULT = "ResultDownloadResource"

  def clearCache(session: Session): Unit = {
    session.getUserProperties
      .entrySet()
      .removeIf(entry => entry.getKey.startsWith(SESSION_CACHED_RESULT))
  }

  def apply(
      session: Session,
      request: ResultDownloadRequest,
      sessionResults: Map[String, List[ITuple]]
  ): ResultDownloadResponse = {
    // look for the response cached in the session to avoid creating duplicated file
    if (
      session.getUserProperties.containsKey(
        getSessionName(request.downloadType)
      )
    ) {
      return session.getUserProperties
        .get(getSessionName(request.downloadType))
        .asInstanceOf[ResultDownloadResponse]
    }

    // By now the workflow should finish running. Only one operator should contain results.
    val count = sessionResults.count(p => !p._2.isEmpty)
    if (count == 0) {
      return ResultDownloadResponse(
        request.downloadType,
        "",
        "The workflow contains no results"
      )
    } else if (count > 1) {
      return ResultDownloadResponse(
        request.downloadType,
        "",
        "The workflow does not finish running"
      )
    }
    val result: List[ITuple] =
      sessionResults.map(p => p._2).find(p => !p.isEmpty).get

    var response: ResultDownloadResponse = null
    request.downloadType match {
      case "google_sheet" =>
        response = handleGoogleSheetRequest(request, result)
      case _ =>
        response = ResultDownloadResponse(
          request.downloadType,
          "",
          s"Unknown download type: ${request.downloadType}"
        )
    }

    // cached the download response in the session
    if (!response.link.isBlank) {
      session.getUserProperties
        .put(getSessionName(request.downloadType), response)
    }
    response
  }

  private def getSessionName(downloadType: String): String = {
    s"${SESSION_CACHED_RESULT}_${downloadType}"
  }

  private def handleGoogleSheetRequest(
      resultDownloadRequest: ResultDownloadRequest,
      result: List[ITuple]
  ): ResultDownloadResponse = {
    // create google sheet
    val sheetService: Sheets = GoogleResource.createSheetService()
    val sheetId: String =
      createGoogleSheet(sheetService, resultDownloadRequest.workflowName)
    if (sheetId == null)
      return ResultDownloadResponse(
        resultDownloadRequest.downloadType,
        "",
        "Fail to create google sheet"
      )

    // upload the schema
    val schema: util.List[AnyRef] = getSchema(result)
    val schemaContent: util.List[util.List[AnyRef]] = Lists.newArrayList()
    schemaContent.add(schema)
    val response: AppendValuesResponse =
      uploadContent(sheetService, sheetId, schemaContent)

    // allow user to access this sheet in the service account
    val drive: Drive = GoogleResource.createDriveService()
    val sharePermission: Permission = new Permission()
      .setType("anyone")
      .setRole("reader")
    drive
      .permissions()
      .create(sheetId, sharePermission)
      .execute()

    // upload the content asynchronously to avoid long waiting on the user side.
    // may change to thread pool
    new Thread(new SheetUploadTask(sheetService, sheetId, result)).start()

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

  private def getSchema(result: List[ITuple]): util.List[AnyRef] = {
    if (result.isEmpty) {
      // should not reach here. Empty check should be done before calling this function
      return Lists.newArrayList()
    }
    // get first ITuple result and get schema from it.
    result(0)
      .asInstanceOf[Tuple]
      .getSchema
      .getAttributeNames()
      .asInstanceOf[util.List[AnyRef]]
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

  private class SheetUploadTask(
      val sheetService: Sheets,
      val sheetId: String,
      val result: List[ITuple]
  ) extends Runnable {
    private final val UPLOAD_SIZE = 100;

    override def run(): Unit = {
      val content: util.List[util.List[AnyRef]] =
        Lists.newArrayListWithCapacity(UPLOAD_SIZE)
      // use for loop to avoid copying the whole result at the same time
      for (tuple: ITuple <- result) {
        val tupleContent: util.List[AnyRef] =
          tuple.asInstanceOf[Tuple].getFields
        content.add(tupleContent)

        if (content.size() == UPLOAD_SIZE) {
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
  }

}
