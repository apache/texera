package edu.uci.ics.texera.web.resource;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.uci.ics.texera.Utils;


import java.io.*;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;


public class GoogleResource {
    private static final int GOOGLE_TIMEOUT_IN_MS = 10000;
    private static final String APPLICATION_NAME = "Texera";
    private static final Config config = ConfigFactory.load("google_api");
    private static final String TOKENS_DIRECTORY_PATH = Utils.amberHomePath()
            .resolve("../conf").resolve(config.getString("google.tokenPath")).toString();
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    /**
     * Global instance of the scopes required by this quickstart.
     * If modifying these scopes, delete your previously saved tokens/ folder.
     */
    private static final String CREDENTIALS_FILE_PATH = Utils.amberHomePath()
            .resolve("../conf").resolve(config.getString("google.credentialPath")).toString();
    private static final List<String> SCOPES = Arrays.asList(SheetsScopes.SPREADSHEETS, DriveScopes.DRIVE);

    // singleton service
    private static Sheets sheetService;
    private static Drive driveService;
    private static Credential serviceAccountCredential;
    private static NetHttpTransport httpTransport;

    private static HttpRequestInitializer createHttpRequestInitializer(final HttpRequestInitializer requestInitializer) {
        // gives timeout on Google APIs.
        return httpRequest -> {
            requestInitializer.initialize(httpRequest);
            httpRequest.setConnectTimeout(GOOGLE_TIMEOUT_IN_MS);
            httpRequest.setReadTimeout(GOOGLE_TIMEOUT_IN_MS);
        };
    }

    /**
     * create a google sheet service to the service account
     */
    public static Sheets getSheetService() throws IOException, GeneralSecurityException {

        if (sheetService == null) {
            synchronized (GoogleResource.class) {
                if (sheetService == null) {
                    sheetService = new Sheets.Builder(getHttpTransport(), JSON_FACTORY, createHttpRequestInitializer(getCredentials()))
                            .setApplicationName(APPLICATION_NAME)
                            .build();
                }
            }
        }
        return sheetService;
    }

    /**
     * create a google drive service to the service account
     */
    public static Drive getDriveService() throws IOException, GeneralSecurityException {
        if (driveService == null) {
            synchronized (GoogleResource.class) {
                if (driveService == null) {
                    driveService = new Drive.Builder(getHttpTransport(), JSON_FACTORY, getCredentials())
                            .setApplicationName(APPLICATION_NAME)
                            .build();
                }
            }
        }
        return driveService;
    }

    /**
     * get the service account's credential
     * rewrite this part when migrate to user account
     */
    private static Credential getCredentials() throws IOException, GeneralSecurityException {
        if (serviceAccountCredential == null) {
            synchronized (GoogleResource.class) {
                if (serviceAccountCredential == null) {
                    serviceAccountCredential = createCredential();
                }
            }
        }
        return serviceAccountCredential;
    }

    private static NetHttpTransport getHttpTransport() throws GeneralSecurityException, IOException {
        if (httpTransport == null) {
            synchronized (GoogleResource.class) {
                if (httpTransport == null) {
                    httpTransport = GoogleNetHttpTransport.newTrustedTransport();
                }
            }
        }
        return httpTransport;
    }

    /**
     * Creates an authorized Credential object for the service account
     * Copied from google sheet API (https://developers.google.com/sheets/api/quickstart/java)
     * rewrite this part when migrate to user's account
     */
    private static Credential createCredential() throws IOException, GeneralSecurityException {
        // Load client secrets
        File initialFile = new File(CREDENTIALS_FILE_PATH);
        InputStream targetStream = new FileInputStream(initialFile);
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(targetStream));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                getHttpTransport(), JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }
}
