package edu.uci.ics.amber.engine.common

import akka.actor.ExtendedActorSystem
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure
import io.altoo.akka.serialization.kryo.DefaultKryoInitializer
import io.altoo.akka.serialization.kryo.serializer.scala.ScalaKryo

import java.lang.invoke.SerializedLambda

class AmberKryoInitializer extends DefaultKryoInitializer {
  override def preInit(kryo: ScalaKryo, system: ExtendedActorSystem): Unit = {
    kryo.register(classOf[SerializedLambda])
    kryo.register(classOf[Closure], new ClosureSerializer())
  }
}
