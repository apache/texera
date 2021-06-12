package edu.uci.ics.texera.web.resource.auth;

import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;


/**
 * referenced from https://stackoverflow.com/questions/36496308/get-user-profile-from-googleidtoken
 * The TOKEN_SERVER_URL of GoogleAuthorizationCodeTokenRequest is "https://oauth2.googleapis.com/token",
 * which will not return user's information other than user id, email and email verified.
 */
public class GoogleAuthorizationCodeTokenV4Request extends GoogleAuthorizationCodeTokenRequest {
    public GoogleAuthorizationCodeTokenV4Request(HttpTransport transport, JsonFactory jsonFactory, String clientId, String
            clientSecret, String code, String redirectUri) {
        super(transport, jsonFactory, "https://www.googleapis.com/oauth2/v4/token", clientId, clientSecret,
                code, redirectUri);
    }
}
