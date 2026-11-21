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

package org.apache.texera.amber.engine.common.ambermessage

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.SerializationExtension
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.util.ArrowUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

/**
  * A LocalColumnarBatch carries a live, non-serializable VectorSchemaRoot. In-JVM it is
  * passed by reference and never serialized, but a checkpoint of an in-flight batch would
  * serialize it, so the custom Kryo serializer (AmberKryoInitializer) must convert it to
  * Arrow bytes and back correctly. These run it through the real Pekko/Kryo stack.
  */
class LocalColumnarBatchSerdeSpec
    extends TestKit(ActorSystem("LocalColumnarBatchSerde", AmberRuntime.pekkoConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  private val serde = SerializationExtension(system)
  private val schema: Schema =
    Schema(
      List(new Attribute("k", AttributeType.INTEGER), new Attribute("v", AttributeType.STRING))
    )

  private def rows: Array[Tuple] =
    (0 until 50)
      .map(i => Tuple.builder(schema).addSequentially(Array[Any](Int.box(i), s"row-$i")).build())
      .toArray

  "a LocalColumnarBatch" should "survive Pekko/Kryo serialization as the same rows" in {
    val original = rows
    val root = ArrowUtils.deserializeRootOwned(
      ArrowUtils.serializeTuples(schema, original),
      ArrowUtils.sharedAllocator
    )
    val batch = LocalColumnarBatch(root, root.getRowCount, schema)

    val wire = serde.serialize(batch).get
    val restored = serde.deserialize(wire, classOf[LocalColumnarBatch]).get

    assert(restored.rowCount == original.length)
    assert(restored.schema == schema)
    val decoded = ArrowUtils.tuplesOfRoot(restored.root)
    assert(decoded.length == original.length)
    decoded.zip(original).foreach {
      case (got, exp) =>
        assert(got.getField[Any]("k") == exp.getField[Any]("k"))
        assert(got.getField[Any]("v") == exp.getField[Any]("v"))
    }
    root.close()
    restored.root.close()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
