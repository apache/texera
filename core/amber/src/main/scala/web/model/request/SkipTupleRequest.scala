package web.model.request

import web.model.common.FaultedTupleFrontend

case class SkipTupleRequest (actorPath:String, faultedTuple:FaultedTupleFrontend) extends TexeraWsRequest
