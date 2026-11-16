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
import com.esotericsoftware.kryo.kryo5.{Kryo, Serializer => Kryo5Serializer}
import io.altoo.serialization.kryo.pekko.DefaultKryoInitializer
import io.altoo.serialization.kryo.scala.serializer.ScalaKryo
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.engine.common.ambermessage.LocalColumnarBatch
import org.apache.texera.amber.util.ArrowUtils

import java.lang.invoke.SerializedLambda

class AmberKryoInitializer extends DefaultKryoInitializer {
  override def preInit(kryo: ScalaKryo): Unit = {
    kryo.register(classOf[SerializedLambda])
    kryo.register(classOf[ClosureSerializer.Closure], new ClosureSerializer())
    // A LocalColumnarBatch normally travels in-JVM by reference and never serializes, but a
    // checkpoint of an in-flight batch would. Its live VectorSchemaRoot is not serializable,
    // so convert it to/from Arrow IPC bytes here: correct, just no longer zero-copy. The
    // rebuilt root is owned by whoever consumes the batch (it closes it).
    kryo.register(classOf[LocalColumnarBatch], new LocalColumnarBatchSerializer())
  }
}

private class LocalColumnarBatchSerializer extends Kryo5Serializer[LocalColumnarBatch] {
  override def write(kryo: Kryo, output: Output, batch: LocalColumnarBatch): Unit = {
    val bytes = ArrowUtils.serializeRoot(batch.root)
    output.writeInt(batch.rowCount)
    kryo.writeObject(output, batch.schema)
    output.writeInt(bytes.length)
    output.writeBytes(bytes)
  }

  override def read(
      kryo: Kryo,
      input: Input,
      tpe: Class[_ <: LocalColumnarBatch]
  ): LocalColumnarBatch = {
    val rowCount = input.readInt()
    val schema = kryo.readObject(input, classOf[Schema])
    val len = input.readInt()
    val bytes = input.readBytes(len)
    LocalColumnarBatch(
      ArrowUtils.deserializeRootOwned(bytes, ArrowUtils.sharedAllocator),
      rowCount,
      schema
    )
  }
}
