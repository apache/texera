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

package org.apache.texera.amber.operator.source

import org.scalatest.flatspec.AnyFlatSpec

import java.io.ByteArrayInputStream

class BufferedBlockReaderSpec extends AnyFlatSpec {

  private def bais(s: String) = new ByteArrayInputStream(s.getBytes("UTF-8"))

  // The reader only ever calls read(byte[]) and close(); each chunk arrives
  // as one read so buffer-boundary paths can be forced without 4 KB inputs.
  // Reads are bounded by the buffer length, carrying any chunk remainder
  // into the next call per the InputStream contract.
  private class ChunkedInputStream(chunks: Seq[String]) extends java.io.InputStream {
    private val iterator = chunks.iterator
    private var pending: Array[Byte] = Array.emptyByteArray
    override def read(): Int = -1
    override def read(buffer: Array[Byte]): Int = {
      if (pending.isEmpty) {
        if (!iterator.hasNext) return -1
        pending = iterator.next().getBytes("UTF-8")
      }
      val length = math.min(pending.length, buffer.length)
      System.arraycopy(pending, 0, buffer, 0, length)
      pending = pending.drop(length)
      length
    }
  }

  // ----- readLine: splitting -----

  "readLine" should "split a line into fields by the delimiter" in {
    val reader = new BufferedBlockReader(bais("a,b,c\n"), 100L, ',', null)
    assert(reader.readLine().toSeq == Seq("a", "b", "c"))
  }

  it should "return consecutive lines and then null at end of stream" in {
    val reader = new BufferedBlockReader(bais("a,b\nc,d\n"), 100L, ',', null)
    assert(reader.readLine().toSeq == Seq("a", "b"))
    assert(reader.readLine().toSeq == Seq("c", "d"))
    assert(reader.readLine() == null)
  }

  it should "return null for an empty stream" in {
    val reader = new BufferedBlockReader(bais(""), 100L, ',', null)
    assert(reader.readLine() == null)
  }

  it should "flush the trailing field at end of stream without a newline" in {
    val reader = new BufferedBlockReader(bais("a,b"), 100L, ',', null)
    assert(reader.readLine().toSeq == Seq("a", "b"))
  }

  it should "flush the trailing field even when it is filtered out by kept" in {
    val reader = new BufferedBlockReader(bais("a,bcd"), 100L, ',', Array(0))
    assert(reader.readLine().toSeq == Seq("a", "bcd"))
  }

  it should "treat carriage returns as line terminators" in {
    val reader = new BufferedBlockReader(bais("x\ry\r"), 100L, ',', null)
    assert(reader.readLine().toSeq == Seq("x"))
    assert(reader.readLine().toSeq == Seq("y"))
  }

  it should "emit a single-null line between the CR and LF of a CRLF terminator" in {
    val reader = new BufferedBlockReader(bais("a\r\nb\n"), 100L, ',', null)
    assert(reader.readLine().toSeq == Seq("a"))
    val crlfArtifact = reader.readLine()
    assert(crlfArtifact.length == 1 && crlfArtifact(0) == null)
    assert(reader.readLine().toSeq == Seq("b"))
  }

  it should "map empty fields to null" in {
    val reader = new BufferedBlockReader(bais("a,,c\n"), 100L, ',', null)
    assert(reader.readLine().toSeq == Seq("a", null, "c"))
    val trailing = new BufferedBlockReader(bais("a,\n"), 100L, ',', null)
    assert(trailing.readLine().toSeq == Seq("a", null))
  }

  // ----- readLine: buffer boundaries -----

  it should "concatenate a field that spans buffer reads" in {
    val reader = new BufferedBlockReader(new ChunkedInputStream(Seq("a", "b\n")), 100L, ',', null)
    assert(reader.readLine().toSeq == Seq("ab"))
  }

  it should "close a carried field when the next buffer starts with a delimiter" in {
    val reader =
      new BufferedBlockReader(new ChunkedInputStream(Seq("ab", ",x\n")), 100L, ',', null)
    assert(reader.readLine().toSeq == Seq("ab", "x"))
  }

  // ----- readLine: kept filter -----

  it should "keep only the requested field indices" in {
    val reader = new BufferedBlockReader(bais("a,b,c\n"), 100L, ',', Array(0, 2))
    assert(reader.readLine().toSeq == Seq("a", "c"))
  }

  it should "return an empty line when every field is filtered out" in {
    val reader = new BufferedBlockReader(bais("a,b\n"), 100L, ',', Array(5))
    assert(reader.readLine().length == 0)
  }

  // ----- hasNext -----

  "hasNext" should "hold until the reader passes the block boundary or hits end of stream" in {
    val fresh = new BufferedBlockReader(bais("ab,c\n"), 5L, ',', null)
    assert(fresh.hasNext)

    // the reader intentionally reads one line past the block boundary
    val pastBoundary = new BufferedBlockReader(bais("ab,c\n"), 4L, ',', null)
    pastBoundary.readLine()
    assert(!pastBoundary.hasNext)

    val atEof = new BufferedBlockReader(bais("a\n"), 100L, ',', null)
    assert(atEof.readLine().toSeq == Seq("a"))
    assert(atEof.hasNext)
    assert(atEof.readLine() == null)
    assert(!atEof.hasNext)
  }

  // ----- close -----

  "close" should "close the underlying stream" in {
    var closed = false
    val input = new ByteArrayInputStream("x".getBytes("UTF-8")) {
      override def close(): Unit = {
        closed = true
        super.close()
      }
    }
    new BufferedBlockReader(input, 10L, ',', null).close()
    assert(closed)
  }
}
