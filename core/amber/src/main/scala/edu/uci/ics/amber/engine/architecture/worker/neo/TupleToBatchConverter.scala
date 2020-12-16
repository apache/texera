package edu.uci.ics.amber.engine.architecture.worker.neo

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.worker.DataTransferSupport

// use the old data transfer support for now since I don't want to change too much
class TupleToBatchConverter(sender: ActorRef) extends DataTransferSupport(sender)
