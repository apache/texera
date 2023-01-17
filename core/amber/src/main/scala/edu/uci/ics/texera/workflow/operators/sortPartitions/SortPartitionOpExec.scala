package edu.uci.ics.texera.workflow.operators.sortPartitions

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.{PauseManager, PauseType}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AcceptMutableStateHandler.AcceptMutableState
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.{Constants, InputExhausted}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}

import scala.collection.mutable.ArrayBuffer

class SortPartitionOpExec(
    sortAttributeName: String,
    operatorSchemaInfo: OperatorSchemaInfo,
    localIdx: Int,
    domainMin: Long,
    domainMax: Long,
    numberOfWorkers: Int
) extends OperatorExecutor {

  var ownTuples: ArrayBuffer[Tuple] = _

  // used by helper workers to store tuples of skewed workers
  var skewedWorkerTuples: ArrayBuffer[Tuple] = _
  var skewedWorkerIdentity: ActorVirtualIdentity = null
  val helperToSkewedBatchSize = 4000

  // used by skewed worker
  var waitingForTuplesFromHelper: Boolean = false
  var numOfBatchesReceivedFromHelper: Int = 0
  var helperWorkerIdentity: ActorVirtualIdentity = null

  def getWorkerIdxForKey(key: Long): Int = {
    val keysPerReceiver = ((domainMax - domainMin) / numberOfWorkers).toLong + 1
    if (key < domainMin) {
      return 0
    }
    if (key > domainMax) {
      return numberOfWorkers - 1
    }
    ((key - domainMin) / keysPerReceiver).toInt
  }

  /**
    * Used by skewed worker to accept tuples from helper worker. When all tuples have been received, this
    * function returns true.
    *
    * Returns: Whether all tuples have been received
    */
  def mergeIntoStoredTuplesList(tuples: ArrayBuffer[Tuple], totalMessagesToExpect: Int): Boolean = {
    try {
      ownTuples.appendAll(tuples)
      numOfBatchesReceivedFromHelper += 1
      if (numOfBatchesReceivedFromHelper == totalMessagesToExpect) {
        waitingForTuplesFromHelper = false
        true
      } else {
        false
      }
    } catch {
      case exception: Exception =>
        false
    }
  }

  def getListsToSendToSkewed(): ArrayBuffer[ArrayBuffer[Tuple]] = {
    val sendingLists = new ArrayBuffer[ArrayBuffer[Tuple]]
    var count = 1
    var curr = new ArrayBuffer[Tuple]
    for (t <- skewedWorkerTuples) {
      curr.append(t)
      if (count % helperToSkewedBatchSize == 0) {
        sendingLists.append(curr)
        curr = new ArrayBuffer[Tuple]
      }
      count += 1
    }
    if (!curr.isEmpty) sendingLists.append(curr)
    sendingLists
  }

  def sendTuplesToSkewedWorker(asyncRPCClient: AsyncRPCClient): Unit = {
    val skewedFutures = new ArrayBuffer[Future[Boolean]]()
    val listsToSend = getListsToSendToSkewed()
    listsToSend.foreach(list => {
      skewedFutures.append(
        asyncRPCClient.send(
          AcceptMutableState(list, listsToSend.size),
          skewedWorkerIdentity
        )
      )
    })
    Future
      .collect(skewedFutures)
      .onSuccess(s => {
        if (s.contains(false)) {
          throw new WorkflowRuntimeException("Error in sending tuples to skewed worker of Sort")
        }
      })
  }

  def sortTuples(): Iterator[Tuple] = ownTuples.sortWith(compareTuples).iterator

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        if (Constants.reshapeSkewHandlingEnabled) {
          val attributeType = t.getSchema().getAttribute(sortAttributeName).getType()
          val attributeIndex = t.getSchema().getIndex(sortAttributeName)
          var workerIdxForKey = -1
          attributeType match {
            case AttributeType.LONG =>
              workerIdxForKey = getWorkerIdxForKey(t.getLong(attributeIndex))
            case AttributeType.INTEGER =>
              workerIdxForKey = getWorkerIdxForKey(t.getInt(attributeIndex).toLong)
            case AttributeType.DOUBLE =>
              workerIdxForKey = getWorkerIdxForKey(t.getDouble(attributeIndex).toLong)
            case _ =>
              throw new RuntimeException(
                "unsupported attribute type in SortOpExec: " + attributeType.toString()
              )
          }
          if (workerIdxForKey == localIdx) {
            ownTuples.append(t)
          } else {
            skewedWorkerTuples.append(t)
          }
        } else {
          ownTuples.append(t)
        }

        Iterator()
      case Right(_) =>
        if (Constants.reshapeSkewHandlingEnabled) {
          if (helperWorkerIdentity == null && skewedWorkerIdentity == null) {
            // this worker is neither the skewed worker nor the helper worker
            sortTuples()
          } else if (helperWorkerIdentity != null) {
            // this worker is the skewed worker.
            if (!waitingForTuplesFromHelper) {
              // It has received the state and can output the results
              sortTuples()
            } else {
              // It will pause its execution here. The execution will be resumed once the state is received
              // from the helper worker
              pauseManager.recordRequest(PauseType.OperatorLogicPause, true)
              Iterator()
            }
          } else if (skewedWorkerIdentity != null) {
            // this worker is the helper worker. It needs to send state to the skewed worker.
            sendTuplesToSkewedWorker(asyncRPCClient)
            sortTuples()
          } else {
            // shouldn't arrive here
            Iterator()
          }
        } else {
          sortTuples()
        }

    }
  }

  def compareTuples(t1: Tuple, t2: Tuple): Boolean = {
    val attributeType = t1.getSchema().getAttribute(sortAttributeName).getType()
    val attributeIndex = t1.getSchema().getIndex(sortAttributeName)
    attributeType match {
      case AttributeType.LONG =>
        t1.getLong(attributeIndex) < t2.getLong(attributeIndex)
      case AttributeType.INTEGER =>
        t1.getInt(attributeIndex) < t2.getInt(attributeIndex)
      case AttributeType.DOUBLE =>
        t1.getDouble(attributeIndex) < t2.getDouble(attributeIndex)
      case _ =>
        true // unsupported type
    }
  }

  override def open = {
    ownTuples = new ArrayBuffer[Tuple]()
    skewedWorkerTuples = new ArrayBuffer[Tuple]()
  }

  override def close = {
    ownTuples.clear()
    skewedWorkerTuples.clear()
  }

}
