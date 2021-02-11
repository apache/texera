package edu.uci.ics.texera.web.resource

import java.io.IOException
import java.util
import collection.JavaConverters._

import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model.{AppendValuesResponse, Spreadsheet, SpreadsheetProperties, ValueRange}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.model.event.ResultDownloadResponse
import edu.uci.ics.texera.web.model.request.ResultDownloadRequest
import edu.uci.ics.texera.workflow.common.tuple.Tuple

object ResultDownloadResource {

  def apply(resultDownloadRequest: ResultDownloadRequest, sessionResults: Map[String, List[ITuple]]): ResultDownloadResponse ={
    val result = getResult(sessionResults)
    resultDownloadRequest.downloadType match {
      case "google_sheet" =>
        downloadGoogleSheet(result)
    }
  }

  private def getResult(sessionResults: Map[String, List[ITuple]]): Array[ITuple] = {
    // Collect the results in the workflow. By now the workflow should already finish running.
    sessionResults.flatMap(pair => pair._2).toArray
  }


  private def downloadGoogleSheet(content: Array[ITuple]): ResultDownloadResponse ={
    // TODO change title
    val title: String = String.valueOf(System.currentTimeMillis)
    val sheetService: Sheets = GoogleResource.createGoogleSheetService();

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
      println("Spreadsheet ID: " + spreadsheetId)

      // convert the tuple content into java list of string
      val temp: scala.collection.mutable.WrappedArray[util.List[AnyRef]] = content
        .map(tuple => tuple.asInstanceOf[Tuple].getFields)
      val values: util.List[util.List[AnyRef]] = temp.asJava

      val body: ValueRange = new ValueRange().setValues(values)
      val range: String = "A1"
      val valueInputOption: String = "RAW"
      val response: AppendValuesResponse = sheetService
        .spreadsheets
        .values
        .append(spreadsheetId, range, body)
        .setValueInputOption(valueInputOption)
        .execute
      System.out.printf("%d cells appended.", response.getUpdates.getUpdatedCells)
    } catch {
      case e: IOException =>
        throw new RuntimeException("io fail", e)
    }

    ResultDownloadResponse("success")
  }
}
