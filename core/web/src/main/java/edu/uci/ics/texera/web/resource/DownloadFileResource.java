package edu.uci.ics.texera.web.resource;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import com.google.api.client.util.Lists;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.ValueRange;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.csv.CSVSink;
import edu.uci.ics.texera.dataflow.sink.csv.CSVSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.excel.ExcelSink;
import edu.uci.ics.texera.dataflow.sink.excel.ExcelSinkPredicate;
import edu.uci.ics.texera.dataflow.sink.json.JSONSink;
import edu.uci.ics.texera.dataflow.sink.json.JSONSinkPredicate;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;

@Path("/download")
@Produces("application/vnd.ms-excel")
@Consumes(MediaType.APPLICATION_JSON)
public class DownloadFileResource {
    
    @GET
    @Path("/result")
    public Response downloadFile(@QueryParam("resultID") String resultID, @QueryParam("downloadType") String downloadType) 
    		throws JsonParseException, JsonMappingException, IOException {        
        java.nio.file.Path resultFile = QueryPlanResource.resultDirectory.resolve(resultID + ".json");

        if (Files.notExists(resultFile)) {
            System.out.println(resultFile + " file does not found");
            return Response.status(Status.NOT_FOUND).build();
        }


        ArrayList<Tuple> result = new ObjectMapper().readValue(Files.readAllBytes(resultFile),
                TypeFactory.defaultInstance().constructCollectionLikeType(ArrayList.class, Tuple.class));

        if (result.size() == 0) {
            System.out.println(resultFile + " file is empty");
            return Response.status(Status.NOT_FOUND).build();
        }

//        List<Tuple> result = WorkflowWebsocketResource.getResult(resultID);
//        List<Tuple> result2 = edu.uci.ics.texera.web.resource.WorkflowWebsocketResource.getResult(resultID);
        
        
        TupleSourceOperator tupleSource = new TupleSourceOperator(result, result.get(0).getSchema());
        if (downloadType.equals("json")) {
        	return downloadJSONFile(tupleSource);
        } else if (downloadType.equals("csv")) {
        	return downloadCSVFile(tupleSource);
        } else if (downloadType.equals("xlsx")) {
        	return downloadExcelFile(tupleSource);
        } else if (downloadType.equals("google_sheet")){
            return uploadToGoogleSheet(tupleSource);
        }
        
        System.out.println("Download type " + downloadType + " is unavailable");
        return Response.status(Status.NOT_FOUND).build();
        
    }

    private Response uploadToGoogleSheet(TupleSourceOperator tupleSource) {
        ExcelSink excelSink = new ExcelSinkPredicate().newOperator();
        excelSink.setInputOperator(tupleSource);
        excelSink.open();
        List<Tuple> tupleList = excelSink.collectAllTuples();
        excelSink.close();

        uploadToGoogleSheet0(tupleList);

        return Response.ok()
                .build();
    }

    private void uploadToGoogleSheet0(List<Tuple> tupleList){
        // TODO change title
        String title = String.valueOf(System.currentTimeMillis());
        Sheets sheet = GoogleResource.createGoogleSheetService();

        Spreadsheet spreadsheet = new Spreadsheet()
                .setProperties(new SpreadsheetProperties()
                        .setTitle(title));
        try {
            spreadsheet = sheet.spreadsheets().create(spreadsheet)
                    .setFields("spreadsheetId")
                    .execute();
            String spreadsheetId = spreadsheet.getSpreadsheetId();
            System.out.println("Spreadsheet ID: " + spreadsheetId);

            List<List<Object>> values = tupleList.stream()
                    .map(Tuple::getFields)
                    .map(iFieldList -> iFieldList.stream()
                            .map(IField::getValue)
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());

            ValueRange body = new ValueRange()
                    .setValues(values);
            String range = "A1";
            String valueInputOption = "RAW";
            AppendValuesResponse result =
                    sheet.spreadsheets().values().append(spreadsheetId, range, body)
                            .setValueInputOption(valueInputOption)
                            .execute();
            System.out.printf("%d cells appended.", result.getUpdates().getUpdatedCells());
        } catch (IOException e){
            throw new TexeraException("io fail");
        }
    }
    
    private Response downloadExcelFile(TupleSourceOperator tupleSource) {
      ExcelSink excelSink = new ExcelSinkPredicate().newOperator();
      excelSink.setInputOperator(tupleSource);
      excelSink.open();
      excelSink.collectAllTuples();
      excelSink.close();  
      
      StreamingOutput fileStream = new StreamingOutput() {
          @Override
          public void write(OutputStream output) throws IOException, WebApplicationException {
              byte[] data = Files.readAllBytes(excelSink.getFilePath());
              output.write(data);
              output.flush();
          }
      };
      return Response.ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
              .header("content-disposition", "attachment; filename=result.xlsx")
              .build();
    }
    
    
    private Response downloadCSVFile(TupleSourceOperator tupleSource) {
        CSVSink csvSink = new CSVSinkPredicate().newOperator();
        csvSink.setInputOperator(tupleSource);
        csvSink.open();
        csvSink.collectAllTuples();
        csvSink.close();  
        
        StreamingOutput fileStream = new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                byte[] data = Files.readAllBytes(csvSink.getFilePath());
                output.write(data);
                output.flush();
            }
        };
        return Response.ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition", "attachment; filename=result.csv")
                .build();
    }
    
    
    private Response downloadJSONFile(TupleSourceOperator tupleSource) {
        JSONSink jsonSink = new JSONSinkPredicate().newOperator();
        jsonSink.setInputOperator(tupleSource);
        jsonSink.open();
        jsonSink.collectAllTuples();
        jsonSink.close();  
        
        StreamingOutput fileStream = new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                byte[] data = Files.readAllBytes(jsonSink.getFilePath());
                output.write(data);
                output.flush();
            }
        };
        return Response.ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition", "attachment; filename=result.json")
                .build();
    }

}
