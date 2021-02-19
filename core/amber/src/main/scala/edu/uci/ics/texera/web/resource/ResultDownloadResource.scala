package edu.uci.ics.texera.web.resource

import java.io.IOException
import java.util
import java.util.stream.Collectors

import com.google.api.services.drive.Drive
import com.google.api.services.drive.model.Permission

import collection.JavaConverters._
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model.{AppendValuesResponse, Spreadsheet, SpreadsheetProperties, ValueRange}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.model.event.ResultDownloadResponse
import edu.uci.ics.texera.web.model.request.ResultDownloadRequest
import edu.uci.ics.texera.workflow.common.tuple.Tuple

object ResultDownloadResource {

  def apply(resultDownloadRequest: ResultDownloadRequest, sessionResults: Map[String, List[ITuple]]): ResultDownloadResponse = {
    val result = getResult(sessionResults)
    resultDownloadRequest.downloadType match {
      case "google_sheet" =>
        createGoogleSheet(resultDownloadRequest, result)
    }
  }

  private def getResult(sessionResults: Map[String, List[ITuple]]): Array[ITuple] = {
    // Collect the results in the workflow. By now the workflow should already finish running.
    sessionResults.flatMap(pair => pair._2).toArray
  }


  private def createGoogleSheet(resultDownloadRequest: ResultDownloadRequest, content: Array[ITuple]): ResultDownloadResponse = {
    val sheetService: Sheets = GoogleResource.createSheetService()
    val title: String = resultDownloadRequest.workflowName

    try {
      // create sheet and get sheetId
      val createSheetRequest = new Spreadsheet()
        .setProperties(new SpreadsheetProperties().setTitle(title))
      val targetSheet: Spreadsheet = sheetService
        .spreadsheets
        .create(createSheetRequest)
        .setFields("spreadsheetId")
        .execute
      val spreadsheetId: String = targetSheet.getSpreadsheetId

      val body: ValueRange = new ValueRange().setValues(convertContent(content))
      val range: String = "A1"
      val valueInputOption: String = "RAW"
      // create the google sheet in the service account
      val response: AppendValuesResponse = sheetService
        .spreadsheets
        .values
        .append(spreadsheetId, range, body)
        .setValueInputOption(valueInputOption)
        .execute

      // allow user to access the file
      val drive:Drive = GoogleResource.creatDriveService()
      val sharePermission: Permission = new Permission()
        .setType("anyone")
        .setRole("reader")
      drive.permissions()
        .create(spreadsheetId, sharePermission)
        .execute()

      val link: String = s"https://docs.google.com/spreadsheets/d/$spreadsheetId/edit"
      val message: String = s"Results saved to Google Sheet."
      ResultDownloadResponse(resultDownloadRequest.downloadType, link, message)
    } catch {
      case e: IOException =>
        ResultDownloadResponse(resultDownloadRequest.downloadType, "", "Fail to create google sheet: " + e.getMessage)
    }
  }

  /**
   * convert the tuple content into java list
   * because Google Sheet API is written in Java
   */
  private def convertContent(content: Array[ITuple]): util.List[util.List[AnyRef]] = {
    if (content.isEmpty) {
      return new util.ArrayList[util.List[AnyRef]](0)
    }

    val schema: util.List[AnyRef] = content(0).asInstanceOf[Tuple].getSchema.getAttributeNames().asInstanceOf[util.List[AnyRef]]
    // add schema at the top, followed by the result data
    val sheetValues: scala.collection.mutable.WrappedArray[util.List[AnyRef]] = schema +:
      content.map(tuple => tuple.asInstanceOf[Tuple].getFields)
    sheetValues.asJava
  }
}
