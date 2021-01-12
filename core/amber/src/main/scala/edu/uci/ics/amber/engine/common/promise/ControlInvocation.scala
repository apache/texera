package edu.uci.ics.amber.engine.common.promise

import edu.uci.ics.amber.engine.common.ambermessage.neo.ControlPayload

/** The invocation of a control command
  *
  * @param context
  * @param call
  */
case class ControlInvocation(context: PromiseContext, call: ControlCommand[_])
    extends ControlPayload
