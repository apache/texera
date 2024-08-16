package web.resources

import jakarta.websocket.Session

class SharedState {
  // Using Option to safely handle the absence of a session
  @volatile var clientSession: Option[Session] = None
  @volatile var podSession: Option[Session] = None
}