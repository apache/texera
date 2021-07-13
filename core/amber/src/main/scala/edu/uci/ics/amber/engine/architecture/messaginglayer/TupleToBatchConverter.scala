package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy._
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.Function.tupled
import scala.collection.mutable

/** This class is a container of all the transfer policyExecs.
  * @param selfID ActorVirtualIdentity of self.
  * @param dataOutputPort DataOutputPort
  */
class TupleToBatchConverter(
    selfID: ActorVirtualIdentity,
    dataOutputPort: DataOutputPort
) {
  private val policyExecs = mutable.HashMap[LinkIdentity, DataSendingPolicyExec]()

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
    policyExecs.update(policy.policyTag, policyExec)

  }

  /**
    * Push one tuple to the downstream, will be batched by each transfer policy.
    * Should ONLY be called by DataProcessor.
    * @param tuple ITuple to be passed.
    */
  def passTupleToDownstream(tuple: ITuple): Unit = {
    policyExecs.valuesIterator.foreach(policyExec =>
      policyExec.addTupleToBatch(tuple) foreach tupled((to, batch) =>
        dataOutputPort.sendTo(to, batch)
      )
    )
  }

  /* Old API: for compatibility */
  @deprecated
  def resetPolicies(): Unit = {
    policyExecs.values.foreach(_.reset())
  }

  /**
    * Send the last batch and EOU marker to all down streams
    */
  def emitEndOfUpstream(): Unit = {
    policyExecs.values.foreach(policyExec =>
      policyExec.noMore() foreach tupled((to, batch) => dataOutputPort.sendTo(to, batch))
    )
  }

}
