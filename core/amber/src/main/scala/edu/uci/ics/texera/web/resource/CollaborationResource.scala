package edu.uci.ics.texera.web.resource

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.ServletAwareConfigurator
import edu.uci.ics.texera.web.model.collab.event._
import edu.uci.ics.texera.web.model.collab.request._
import edu.uci.ics.texera.web.model.collab.response.HeartBeatResponse
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.CollaborationResource.{
  sessionIdwIdMap,
  wIdLockHolderIdMap,
  wIdSessionIdMap,
  websocketSessionMap
}

import javax.websocket.server.ServerEndpoint
import javax.websocket.{OnClose, OnMessage, OnOpen, Session}
import scala.collection.mutable
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

object CollaborationResource {
  final val websocketSessionMap = new mutable.HashMap[String, Session]()
  final val sessionIdwIdMap = new mutable.HashMap[String, String]()
  final val wIdSessionIdMap = new mutable.HashMap[String, mutable.Set[String]]()
  final val wIdLockHolderIdMap = new mutable.HashMap[String, String]()
}

@ServerEndpoint(
  value = "/wsapi/collab",
  configurator = classOf[ServletAwareConfigurator]
)
class CollaborationResource extends LazyLogging {

  final val objectMapper = Utils.objectMapper

  @OnMessage
  def myOnMsg(senderSession: Session, message: String): Unit = {
    val request = objectMapper.readValue(message, classOf[CollabWebSocketRequest])
    val uidOpt = senderSession.getUserProperties.asScala
      .get(classOf[User].getName)
      .map(_.asInstanceOf[User].getUid)
    request match {
      case wIdRequest: InformWIdRequest =>
        val workflowStateId: String = uidOpt match {
          case Some(user) =>
            val workflowStateId = user + "-" + wIdRequest.wId
            logger.info("New session on workflow with wid: " + workflowStateId)
            workflowStateId
          case None =>
            // use a fixed wid for reconnection
            val workflowStateId = "dummy wid"
            workflowStateId
        }
        sessionIdwIdMap(senderSession.getId) = workflowStateId
        val sessionIdSet: mutable.Set[String] =
          wIdSessionIdMap.get(workflowStateId) match {
            case Some(set) =>
              set.+(senderSession.getId)
            case None =>
              mutable.Set(senderSession.getId)
          }
        wIdSessionIdMap(workflowStateId) = sessionIdSet

      case commandRequest: CommandRequest =>
        logger.info("Received command message: " + commandRequest.commandMessage)
        for (sessionId <- websocketSessionMap.keySet) {
          // only send to other sessions, not the session that sent the message
          val session = websocketSessionMap(sessionId)
          val sessionStateId = sessionIdwIdMap.get(sessionId)
          val senderStateId = sessionIdwIdMap.get(senderSession.getId)
          if (
            session != senderSession && sessionStateId.isDefined && senderStateId.isDefined && senderStateId == sessionStateId
          ) {
            send(session, CommandEvent(commandRequest.commandMessage))
            logger.info("Message propagated to workflow " + sessionStateId.toString)
          }
        }
      case heartbeat: HeartBeatRequest =>
        send(senderSession, HeartBeatResponse())

      case tryLock: TryLockRequest =>
        val sessionId = senderSession.getId
        val wId = sessionIdwIdMap(sessionId)
        if (wId == "dummy wid")
          send(senderSession, LockGrantedEvent())
        else if (
          !wIdLockHolderIdMap.keySet.contains(wId) || wIdLockHolderIdMap(
            wId
          ) == null || wIdLockHolderIdMap(wId) == sessionId
        ) {
          grantLock(senderSession, sessionId, wId)
        } else if (wIdLockHolderIdMap(wId) != sessionId) {
          send(senderSession, LockRejectedEvent())
        }

      case acquireLock: AcquireLockRequest =>
        val senderSessId = senderSession.getId
        val senderWid = sessionIdwIdMap(senderSessId)
        if (wIdLockHolderIdMap(senderWid) != senderSessId) {
          val holderSessId = wIdLockHolderIdMap(senderWid)
          val holderSession = websocketSessionMap(holderSessId)
          send(holderSession, ReleaseLockEvent())
          send(senderSession, LockGrantedEvent())
          wIdLockHolderIdMap(senderWid) = senderSessId
          logger.info("Session " + senderSessId + " has lock on " + senderWid)
        } else {
          send(senderSession, LockGrantedEvent())
        }

      case reloadWorkflow: ReloadWorkflowRequest =>
        for (sessionId <- websocketSessionMap.keySet) {
          // only send to other sessions, not the session that sent the message
          val session = websocketSessionMap(sessionId)
          val sessionStateId = sessionIdwIdMap.get(sessionId)
          val senderStateId = sessionIdwIdMap.get(senderSession.getId)
          if (
            session != senderSession && sessionStateId.isDefined && senderStateId.isDefined && senderStateId == sessionStateId
          ) {
            send(session, ReloadWorkflowEvent())
            logger.info("Reload propagated to workflow " + sessionStateId.toString)
          }
        }
    }
  }

  private def grantLock(session: Session, sessionId: String, wId: String): Unit = {
    wIdLockHolderIdMap(wId) = sessionId
    logger.info("Session " + sessionId + " has lock on " + wId + " now")
    send(session, LockGrantedEvent())
  }

  private def send(session: Session, msg: CollabWebSocketEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(msg))
  }

  @OnOpen
  def myOnOpen(session: Session): Unit = {
    websocketSessionMap += (session.getId -> session)
  }

  @OnClose
  def myOnClose(senderSession: Session): Unit = {
    val senderSessId = senderSession.getId
    websocketSessionMap -= senderSessId
    val senderWid = sessionIdwIdMap(senderSessId)
    wIdSessionIdMap(senderWid) -= senderSessId
    if (wIdLockHolderIdMap(senderWid) == senderSessId) {
      wIdLockHolderIdMap(senderWid) = null
      val set = wIdSessionIdMap(senderWid)
      if (set.nonEmpty) {
        val otherSessId = set.head
        val otherSession = websocketSessionMap(otherSessId)
        grantLock(otherSession, otherSessId, senderWid)
      }
    }
    sessionIdwIdMap -= senderSessId
    logger.info("Session " + senderSessId + " disconnected")
  }
}
