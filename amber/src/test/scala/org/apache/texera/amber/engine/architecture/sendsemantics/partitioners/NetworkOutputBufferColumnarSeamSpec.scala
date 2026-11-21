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

package org.apache.texera.amber.engine.architecture.sendsemantics.partitioners

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import org.apache.texera.amber.engine.common.ambermessage.{
  VectorFrame,
  DataFrame,
  WorkflowFIFOMessage
}
import org.apache.texera.amber.util.ArrowUtils
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer

/**
  * The columnar-wire switch on NetworkOutputBuffer.
  *
  * A NetworkOutputBuffer chooses, on flush, how to put a batch on the wire: as row
  * objects (DataFrame) or as an Arrow columnar block (VectorFrame). The choice is
  * the constructor flag `columnar`; flush() reads exactly that flag. These tests
  * verify the buffer honors the flag on both the manual and the size-triggered
  * flush, carries the row count and schema on the columnar frame, and that a
  * columnar frame's bytes decode back to the very rows that were buffered.
  */
class NetworkOutputBufferColumnarSeamSpec extends AnyFlatSpec {

  private val sender: ActorVirtualIdentity = ActorVirtualIdentity("sender")
  private val receiver: ActorVirtualIdentity = ActorVirtualIdentity("receiver")
  private val vAttr: Attribute = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(vAttr)
  private def tuple(v: Int): Tuple = Tuple.builder(schema).add(vAttr, v).build()

  private def fixture(
      columnar: Boolean,
      batchSize: Int = 1000
  ): (NetworkOutputBuffer, ArrayBuffer[WorkflowFIFOMessage]) = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val gateway = new NetworkOutputGateway(sender, m => { sent += m; () })
    val buffer = new NetworkOutputBuffer(receiver, gateway, batchSize, columnar)
    (buffer, sent)
  }

  it should "emit a VectorFrame carrying the same rows when columnar is on" in {
    val (buffer, sent) = fixture(columnar = true)
    buffer.addTuple(tuple(1))
    buffer.addTuple(tuple(2))
    buffer.addTuple(tuple(3))
    buffer.flush()

    assert(sent.size == 1)
    sent.head.payload match {
      case cf: VectorFrame =>
        assert(cf.rowCount == 3)
        assert(cf.schema == schema)
        // The Arrow bytes decode back to exactly the buffered rows, in order.
        val back = ArrowUtils.deserializeTuples(cf.arrowIpcBytes)
        assert(back.map(_.getField[Any]("v")).toList == List(1, 2, 3))
      case other => fail(s"expected a VectorFrame, got: $other")
    }
  }

  it should "emit a DataFrame (rows) when columnar is off" in {
    val (buffer, sent) = fixture(columnar = false)
    buffer.addTuple(tuple(10))
    buffer.addTuple(tuple(20))
    buffer.flush()

    assert(sent.size == 1)
    sent.head.payload match {
      case df: DataFrame =>
        assert(df.frame.map(_.getField[Any]("v")).toList == List(10, 20))
      case other => fail(s"expected a DataFrame, got: $other")
    }
  }

  it should "send nothing when flushed empty, either way" in {
    val (on, onSent) = fixture(columnar = true)
    on.flush()
    assert(onSent.isEmpty)
    val (off, offSent) = fixture(columnar = false)
    off.flush()
    assert(offSent.isEmpty)
  }

  it should "honor the flag on the size-triggered auto-flush" in {
    val (on, onSent) = fixture(columnar = true, batchSize = 2)
    on.addTuple(tuple(1))
    assert(onSent.isEmpty)
    on.addTuple(tuple(2))
    assert(onSent.size == 1)
    assert(onSent.head.payload.isInstanceOf[VectorFrame])

    val (off, offSent) = fixture(columnar = false, batchSize = 2)
    off.addTuple(tuple(1))
    off.addTuple(tuple(2))
    assert(offSent.size == 1)
    assert(offSent.head.payload.isInstanceOf[DataFrame])
  }
}
