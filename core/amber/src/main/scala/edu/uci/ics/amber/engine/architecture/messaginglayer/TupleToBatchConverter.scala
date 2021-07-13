package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy._
import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.Function.tupled

/** This class is a container of all the transfer policyExecs.
  * @param selfID ActorVirtualIdentity of self.
  * @param dataOutputPort DataOutputPort
  */
class TupleToBatchConverter(
    selfID: ActorVirtualIdentity,
    dataOutputPort: DataOutputPort
) {
  private var policyExecs = new Array[DataSendingPolicyExec](0)

  /**
    * Add down stream operator and its corresponding transfer policy executor.
    * @param policy DataSendingPolicy, describes how and whom to send to.
    */
  def addPolicy(
      policy: DataSendingPolicy
  ): Unit = {

    // create a corresponding policy executor for the given policy
    val policyExec = policy match {
      case oneToOnePolicy: OneToOnePolicy     => OneToOnePolicyExec(oneToOnePolicy)
      case roundRobinPolicy: RoundRobinPolicy => RoundRobinPolicyExec(roundRobinPolicy)
      case hashBasedShufflePolicy: HashBasedShufflePolicy =>
        HashBasedShufflePolicyExec(hashBasedShufflePolicy)
    }

    // update the existing policy executors.
    for (i <- policyExecs.indices) {
      if (policyExecs(i).policy.policyTag == policy.policyTag) {
        policyExecs(i) = policyExec
        return
      }
    }

    // if no existing policy executor is found to be updated, append it.
    policyExecs = policyExecs :+ policyExec

  }

  /**
    * Push one tuple to the downstream, will be batched by each transfer policy.
    * Should ONLY be called by DataProcessor.
    * @param tuple ITuple to be passed.
    */
  def passTupleToDownstream(tuple: ITuple): Unit = {
    var i = 0
    while (i < policyExecs.length) {
      policyExecs(i).addTupleToBatch(tuple) foreach tupled((to, batch) =>
        dataOutputPort.sendTo(to, batch)
      )
      i += 1
    }
  }

  /* Old API: for compatibility */
  @deprecated
  def resetPolicies(): Unit = {
    policyExecs.foreach(_.reset())
  }

  /**
    * Send the last batch and EOU marker to all down streams
    */
  def emitEndOfUpstream(): Unit = {
    var i = 0
    while (i < policyExecs.length) {
      val receiversAndBatches: Array[(ActorVirtualIdentity, DataPayload)] = policyExecs(i).noMore()
      receiversAndBatches.foreach {
        case (id, batch) => dataOutputPort.sendTo(id, batch)
      }
      i += 1
    }
  }

}
