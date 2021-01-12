package edu.uci.ics.amber.engine.common.ambermessage.neo

import edu.uci.ics.amber.engine.common.promise.PromiseContext

trait ControlPayload extends Serializable {
  val context: PromiseContext
}
