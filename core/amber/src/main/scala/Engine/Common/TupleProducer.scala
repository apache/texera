package Engine.Common

import Engine.Common.AmberTuple.Tuple

import scala.collection.mutable
import scala.collection.mutable.HashMap

abstract class TupleProducer {

  var params = new mutable.HashMap[String, String]

  @throws(classOf[Exception])
  def initialize(): Unit = {
    initializeWorker()
    updateParamMap()
  }

  @throws(classOf[Exception])
  def initializeWorker(): Unit

  @throws(classOf[Exception])
  def hasNext: Boolean

  @throws(classOf[Exception])
  def next(): Tuple

  /**
    * Update the @params map with latest variable.
    * Usually called when user is expected to call getParam(). eg: after pausing
    * @throws java.lang.Exception
    */
  @throws(classOf[Exception])
  def updateParamMap(): Unit

  @throws(classOf[Exception])
  def dispose(): Unit

  @throws(classOf[Exception])
  def getParam(query:String): String = {
    params.getOrElse(query, null)
  }

}
