package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic

class SerializationManager {

  @transient var serializationCall: () => Unit = _
  var opInitMsg: InitializeOperatorLogic = _

}
