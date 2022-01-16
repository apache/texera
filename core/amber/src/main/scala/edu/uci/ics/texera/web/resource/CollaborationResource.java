package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.web.ServletAwareConfigurator;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.util.HashMap;


@ServerEndpoint(
        value = "/wsapi/collab",
        configurator = ServletAwareConfigurator.class
)
public class CollaborationResource {
    public static HashMap<String, Session> websocketSessionMap = new HashMap<>();

    @OnOpen
    public void myOnOpen(final Session session) {
        websocketSessionMap.put(session.getId(), session);
    }

    @OnMessage
    public void myOnMsg(final Session senderSession, String message) {
        for(String sessionId: websocketSessionMap.keySet()) {
            // only send to other sessions, not the session that sent the message
            Session session = websocketSessionMap.get(sessionId);
            if (session != senderSession) {
                websocketSessionMap.get(sessionId).getAsyncRemote().sendText(message);
            }
        }
        System.out.println("[COLLAB] message propagated: " + message);
    }

    @OnClose
    public void myOnClose(final Session session) {
        websocketSessionMap.remove(session.getId());
        System.out.println("[COLLAB] session " + session.getId() + " disconnected");
    }
}