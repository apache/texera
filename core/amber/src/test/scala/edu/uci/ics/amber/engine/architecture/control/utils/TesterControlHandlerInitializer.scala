package edu.uci.ics.amber.engine.architecture.control.utils

import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.control.{
  ControlMessageSource,
  ControlHandlerInitializer,
  ControlMessageReceiver
}

class TesterControlHandlerInitializer(
    val myID: ActorVirtualIdentity,
    source: ControlMessageSource,
    receiver: ControlMessageReceiver
) extends ControlHandlerInitializer(source, receiver)
    with PingPongHandler
    with ChainHandler
    with MultiCallHandler
    with CollectHandler
    with NestedHandler
    with RecursionHandler {}
