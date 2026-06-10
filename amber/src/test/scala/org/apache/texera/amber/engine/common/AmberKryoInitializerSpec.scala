/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.engine.common

import com.esotericsoftware.kryo.kryo5.io.{Input, Output}
import com.esotericsoftware.kryo.kryo5.serializers.ClosureSerializer
import com.esotericsoftware.kryo.kryo5.util.{DefaultClassResolver, MapReferenceResolver}
import io.altoo.serialization.kryo.scala.serializer.ScalaKryo
import org.scalatest.flatspec.AnyFlatSpec

import java.lang.invoke.SerializedLambda

class AmberKryoInitializerSpec extends AnyFlatSpec {

  /** A bare ScalaKryo with none of AmberKryoInitializer's registrations applied. */
  private def bareKryo(): ScalaKryo =
    new ScalaKryo(new DefaultClassResolver(), new MapReferenceResolver())

  /** A ScalaKryo configured exactly as production does, via preInit. */
  private def initializedKryo(): ScalaKryo = {
    val kryo = bareKryo()
    // preInit needs neither the actor system nor super.preInit, so a plain
    // instance is enough to reproduce the production registration.
    new AmberKryoInitializer().preInit(kryo)
    kryo
  }

  /**
    * A kryo for round-trip checks. ClosureSerializer writes the closure's
    * capturing class; production registers such classes through its full
    * init/config chain, which is out of scope here, so we drop the
    * registration requirement to isolate the closure path preInit enables.
    */
  private def closureKryo(): ScalaKryo = {
    val kryo = initializedKryo()
    kryo.setRegistrationRequired(false)
    kryo
  }

  private def roundTrip(kryo: ScalaKryo, value: AnyRef): AnyRef = {
    val output = new Output(1024)
    kryo.writeClassAndObject(output, value)
    output.close()
    val input = new Input(output.toBytes)
    val restored = kryo.readClassAndObject(input)
    input.close()
    restored
  }

  "AmberKryoInitializer.preInit" should "register SerializedLambda and the closure serializer" in {
    val kryo = initializedKryo()

    // getClassResolver.getRegistration is a pure lookup: it returns null for
    // classes that were never explicitly registered (unlike Kryo.getRegistration,
    // which implicitly registers on miss).
    val lambdaReg = kryo.getClassResolver.getRegistration(classOf[SerializedLambda])
    assert(lambdaReg != null, "SerializedLambda must be registered")

    val closureReg = kryo.getClassResolver.getRegistration(classOf[ClosureSerializer.Closure])
    assert(closureReg != null, "ClosureSerializer.Closure must be registered")
    assert(
      closureReg.getSerializer.isInstanceOf[ClosureSerializer],
      "the closure class must be bound to a ClosureSerializer"
    )
  }

  it should "not register those classes on a kryo it never touched" in {
    // Guards against the assertions above passing for some unrelated default
    // registration: a bare ScalaKryo knows nothing about lambdas.
    val kryo = bareKryo()
    assert(kryo.getClassResolver.getRegistration(classOf[SerializedLambda]) == null)
    assert(kryo.getClassResolver.getRegistration(classOf[ClosureSerializer.Closure]) == null)
  }

  "A kryo configured by AmberKryoInitializer" should "round-trip a Scala closure" in {
    // Scala 2.13 compiles lambdas as serializable invokedynamic closures, so
    // Kryo routes them through the ClosureSerializer registered in preInit.
    val addend = 41
    val fn: Int => Int = (x: Int) => x + addend

    val restored = roundTrip(closureKryo(), fn).asInstanceOf[Int => Int]
    assert(restored(1) == 42, "the deserialized closure must behave like the original")
  }

  it should "preserve distinct captured state across separate closures" in {
    // The captured value travels inside the SerializedLambda, so two closures
    // built the same way but capturing different values must stay independent.
    val kryo = closureKryo()
    val tenX: Int => Int = { val f = 10; (x: Int) => x * f }
    val hundredX: Int => Int = { val f = 100; (x: Int) => x * f }

    val restoredTen = roundTrip(kryo, tenX).asInstanceOf[Int => Int]
    val restoredHundred = roundTrip(kryo, hundredX).asInstanceOf[Int => Int]
    assert(restoredTen(3) == 30)
    assert(restoredHundred(3) == 300)
  }
}
