package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.collection.mutable

case class WorkerPort(
    schema: Schema,
    // TODO: change it to manage the actual AmberFIFOChannel instead of Boolean
    channels: mutable.HashMap[ChannelIdentity, Boolean] = mutable.HashMap()
)
