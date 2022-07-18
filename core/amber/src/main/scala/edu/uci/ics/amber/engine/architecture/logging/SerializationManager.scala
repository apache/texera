package edu.uci.ics.amber.engine.architecture.logging

import com.twitter.chill.{Kryo, KryoInstantiator, KryoPool}
import edu.uci.ics.amber.engine.architecture.logging.SerializationManager.kryo

object SerializationManager {
  val POOL_SIZE = 10
  val kryo: KryoPool = KryoPool.withByteArrayOutputStream(POOL_SIZE, new KryoInstantiator)
}

class SerializationManager() {

  def serialize(serializedDeterminant: InMemDeterminant): Array[Byte] = {
    kryo.toBytesWithClass(serializedDeterminant)
  }

  def deserialize(x: Array[Byte]): InMemDeterminant = {
    kryo.fromBytes(x).asInstanceOf[InMemDeterminant]
  }

}
