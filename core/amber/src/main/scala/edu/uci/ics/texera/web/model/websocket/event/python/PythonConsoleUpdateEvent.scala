package edu.uci.ics.texera.web.model.websocket.event.python

import com.google.protobuf.timestamp.Timestamp
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.PythonConsoleMessageV2
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent

object PythonConsoleUpdateEvent {
  def apply(
      opId: String,
      workerId: String,
      messages: Seq[PythonConsoleMessageV2]
  ): PythonConsoleUpdateEvent = {
    PythonConsoleUpdateEvent(
      opId,
      messages.map((m: PythonConsoleMessageV2) =>
        PythonWorkerConsoleMessage(workerId, m.timestamp, m.level, m.source, m.message)
      )
    )
  }
}

case class PythonWorkerConsoleMessage(
    workerId: String,
    timestamp: Timestamp,
    level: String,
    source: String,
    message: String
)

case class PythonConsoleUpdateEvent(
    operatorId: String,
    messages: Seq[PythonWorkerConsoleMessage]
) extends TexeraWebSocketEvent
