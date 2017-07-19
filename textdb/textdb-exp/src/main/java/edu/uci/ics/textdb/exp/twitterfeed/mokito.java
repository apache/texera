package edu.uci.ics.textdb.exp.twitterfeed;
import com.twitter.hbc.BasicReconnectionManager;
import com.twitter.hbc.RateTracker;
import com.twitter.hbc.core.HttpConstants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.RawEndpoint;
import com.twitter.hbc.core.processor.HosebirdMessageProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by Chang on 7/18/17.
 */
public class mokito {
    private HttpClient mockClient;
    private HttpResponse mockResponse;
    private HttpEntity mockHttpEntity;
    private StatusLine mockStatusLine;

    private HosebirdMessageProcessor mockProcessor;
    private Authentication mockAuth;
    private RateTracker mockRateTracker;
    private ClientConnectionManager mockConnectionManager;
    private InputStream mockInputStream;
    public void setup() throws Exception {

        mockClient = mock(HttpClient.class);
        mockResponse = mock(HttpResponse.class);
        mockStatusLine = mock(StatusLine.class);

        mockConnectionManager = mock(ClientConnectionManager.class);
        mockRateTracker = mock(RateTracker.class);

        mockInputStream = mock(InputStream.class);
        mockAuth = mock(Authentication.class);

        mockProcessor = mock(HosebirdMessageProcessor.class);

        mockHttpEntity = mock(HttpEntity.class);

        // set up required mocks to mock out all of the clientbase stuff
        when(mockClient.execute(any(HttpUriRequest.class)))
                .thenReturn(mockResponse);
        when(mockClient.getConnectionManager())
                .thenReturn(mockConnectionManager);

        when(mockResponse.getStatusLine())
                .thenReturn(mockStatusLine);
        when(mockResponse.getEntity())
                .thenReturn(mockHttpEntity);
        when(mockHttpEntity.getContent())
                .thenReturn(mockInputStream);
        when(mockStatusLine.getReasonPhrase())
                .thenReturn("reason");
    }

    // These tests are going to get a little hairy in terms of mocking, but doable.
    // Some of the functionality is already tested in ClientBaseTest, but this tests
    // the overall flow. Worth it?


//    public void testIOExceptionDuringProcessing() throws Exception {
//        ClientBase clientBase = new ClientBase("name",
//                mockClient, new HttpHosts("http://hi"), new RawEndpoint("/endpoint", HttpConstants.HTTP_GET), mockAuth,
//                mockProcessor, mockReconnectionManager, mockRateTracker
//        );
//        BasicClient client = new BasicClient(clientBase, executorService);
//        final CountDownLatch latch = new CountDownLatch(1);
//        when(mockStatusLine.getStatusCode())
//                .thenReturn(200);

        //doNothing().when(mockProcessor).setup(any(InputStream.class));
      //  doThrow(new IOException()).
       //         doThrow(new IOException()).
//                doThrow(new IOException()).
//                doAnswer(new Answer() {
//                    @Override
//                    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
//                        latch.countDown();
//                        return null;
//                    }
//                }).when(mockProcessor).process();

//        client.connect();
//        latch.await();
//        assertFalse(clientBase.isDone());
//        verify(mockProcessor, times(4)).setup(any(InputStream.class));
//        // throw 3 exceptions, 4th one keeps going
//        verify(mockProcessor, atLeast(4)).process();
//
//        client.stop();
//        verify(mockConnectionManager, atLeastOnce()).shutdown();
//        assertTrue(client.isDone());
//        assertEquals(EventType.STOPPED_BY_USER, clientBase.getExitEvent().getEventType());
    }
//}
