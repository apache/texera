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

package org.apache.texera.web.service

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.WorkflowIdentity
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.web.model.http.request.result.{OperatorExportInfo, ResultExportRequest}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.net.URI
import java.nio.charset.StandardCharsets
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response

// Unit tests for the ResultExportService request helpers parseOperators and
// validateExportRequest, plus the private export-encoding writers. Constructing
// the service is cheap (it only stores the identity and computing-unit id), and
// every writer takes a VirtualDocument parameter, so an in-spec fake document is
// enough — no Iceberg, MinIO or DB is involved.
class ResultExportServiceSpec extends AnyFlatSpec with Matchers with PrivateMethodTester {

  private val service = new ResultExportService(WorkflowIdentity(1L), computingUnitId = 0)

  private def requestWith(operators: List[OperatorExportInfo]): ResultExportRequest =
    ResultExportRequest(
      exportType = "csv",
      workflowId = 1,
      workflowName = "wf",
      operators = operators,
      datasetIds = List.empty,
      rowIndex = 0,
      columnIndex = 0,
      filename = "",
      computingUnitId = 0
    )

  // -- parseOperators ---------------------------------------------------------

  "parseOperators" should "deserialize a JSON array into OperatorExportInfo objects" in {
    val json =
      """[{"id":"op-1","outputType":"csv"},{"id":"op-2","outputType":"arrow"}]"""
    val parsed = service.parseOperators(json)

    parsed shouldBe List(
      OperatorExportInfo("op-1", "csv"),
      OperatorExportInfo("op-2", "arrow")
    )
  }

  it should "round-trip an empty JSON array to an empty list" in {
    service.parseOperators("[]") shouldBe List.empty[OperatorExportInfo]
  }

  it should "throw when given a malformed JSON string" in {
    a[JsonProcessingException] should be thrownBy service.parseOperators("not json")
  }

  // -- validateExportRequest --------------------------------------------------

  "validateExportRequest" should "return a 400 response when no operators are selected" in {
    val result = service.validateExportRequest(requestWith(List.empty))

    val response = result.getOrElse(fail("expected a validation error response"))
    response.getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
    response.getEntity match {
      case m: java.util.Map[_, _] => m.get("error") shouldBe "No operator selected"
      case other                  => fail(s"unexpected entity: $other")
    }
  }

  it should "return None when at least one operator is selected" in {
    val result =
      service.validateExportRequest(requestWith(List(OperatorExportInfo("op-1", "csv"))))
    result shouldBe None
  }

  // -- fakes for the export writers -------------------------------------------

  private val schema: Schema = Schema()
    .add("name", AttributeType.STRING)
    .add("count", AttributeType.INTEGER)

  private def tupleOf(name: String, count: Int): Tuple =
    Tuple.builder(schema).addSequentially(Array[Any](name, count)).build()

  // A minimal in-memory document overriding only the members the writers touch;
  // every other VirtualDocument method keeps its default throwing body.
  private class FakeDoc(
      rows: Seq[Tuple],
      countOverride: Option[Long] = None,
      stream: Option[InputStream] = None
  ) extends VirtualDocument[Tuple] {
    override def getURI: URI = new URI("file:///stub/export")
    override def clear(): Unit = ()
    override def getCount: Long = countOverride.getOrElse(rows.length.toLong)
    override def get(): Iterator[Tuple] = rows.iterator
    override def getRange(from: Int, until: Int, columns: Option[Seq[String]]): Iterator[Tuple] =
      rows.slice(from, until).iterator
    override def asInputStream(): InputStream = stream.getOrElse(super.asInputStream())
  }

  private class TrackingInputStream(bytes: Array[Byte]) extends ByteArrayInputStream(bytes) {
    var closed = false
    override def close(): Unit = { closed = true; super.close() }
  }

  private class TrackingOutputStream extends ByteArrayOutputStream {
    var closed = false
    override def close(): Unit = { closed = true; super.close() }
  }

  private val streamDocumentAsCSV = PrivateMethod[Unit](Symbol("streamDocumentAsCSV"))
  private val streamDocumentAsArrow = PrivateMethod[Unit](Symbol("streamDocumentAsArrow"))
  private val streamDocumentAsHTML = PrivateMethod[Unit](Symbol("streamDocumentAsHTML"))
  private val streamDocumentAsParquetZip =
    PrivateMethod[Unit](Symbol("streamDocumentAsParquetZip"))
  private val streamCellData = PrivateMethod[Unit](Symbol("streamCellData"))
  private val convertFieldToBytes = PrivateMethod[Array[Byte]](Symbol("convertFieldToBytes"))

  private def utf8(out: ByteArrayOutputStream): String =
    new String(out.toByteArray, StandardCharsets.UTF_8)

  // Assert on parsed lines rather than raw bytes so the CSV library's choice of
  // line terminator does not make these tests brittle.
  private def csvLines(out: ByteArrayOutputStream): List[String] = utf8(out).linesIterator.toList

  // -- streamDocumentAsCSV ----------------------------------------------------

  "streamDocumentAsCSV" should "write nothing when the document is empty" in {
    val out = new ByteArrayOutputStream()
    service invokePrivate streamDocumentAsCSV(new FakeDoc(Seq.empty), out, None)
    out.size shouldBe 0
  }

  it should "write nothing when the iterator yields no rows despite a non-zero count" in {
    val out = new ByteArrayOutputStream()
    val doc = new FakeDoc(Seq.empty, countOverride = Some(2L))
    service invokePrivate streamDocumentAsCSV(doc, out, None)
    out.size shouldBe 0
  }

  it should "infer the header from the first row's schema and still write that row" in {
    val out = new ByteArrayOutputStream()
    val rows = Seq(tupleOf("a", 1), tupleOf("b", 2))
    service invokePrivate streamDocumentAsCSV(new FakeDoc(rows), out, None)

    csvLines(out) shouldBe List("name,count", "a,1", "b,2")
  }

  it should "use the supplied headers without consuming the first row" in {
    val out = new ByteArrayOutputStream()
    val rows = Seq(tupleOf("a", 1), tupleOf("b", 2))
    val headers = Some(List("h1", "h2"))
    service invokePrivate streamDocumentAsCSV(new FakeDoc(rows), out, headers)

    // The supplied header replaces the inferred one, and — unlike the None
    // branch above — the first row is not consumed by header inference, so it
    // still appears in the body.
    csvLines(out) shouldBe List("h1,h2", "a,1", "b,2")
  }

  it should "write every row when the document exceeds the chunk size" in {
    val out = new ByteArrayOutputStream()
    val rowCount = Constants.CHUNK_SIZE * 2 + 5
    val rows = (1 to rowCount).map(i => tupleOf(s"r$i", i))
    service invokePrivate streamDocumentAsCSV(new FakeDoc(rows), out, Some(List("h1", "h2")))

    val lines = csvLines(out)
    lines.head shouldBe "h1,h2"
    lines.tail should have length rowCount
    lines.tail.head shouldBe "r1,1"
    lines.last shouldBe s"r$rowCount,$rowCount"
  }

  // -- streamCellData ---------------------------------------------------------

  "streamCellData" should "reject a rowIndex beyond the document count" in {
    val doc = new FakeDoc(Seq(tupleOf("a", 1), tupleOf("b", 2)))
    val request = requestWith(List(OperatorExportInfo("op-1", "data"))).copy(rowIndex = 5)

    val ex = intercept[WebApplicationException] {
      service invokePrivate streamCellData(new ByteArrayOutputStream(), request, doc)
    }
    ex.getMessage shouldBe "Invalid rowIndex (5). Total rows: 2"
  }

  it should "reject a columnIndex beyond the selected row's field count" in {
    val doc = new FakeDoc(Seq(tupleOf("a", 1), tupleOf("b", 2)))
    val request = requestWith(List(OperatorExportInfo("op-1", "data"))).copy(columnIndex = 2)

    val ex = intercept[WebApplicationException] {
      service invokePrivate streamCellData(new ByteArrayOutputStream(), request, doc)
    }
    ex.getMessage shouldBe "Invalid columnIndex (2). Total columns: 2"
  }

  it should "fail when the row cannot be retrieved despite a non-zero count" in {
    val doc = new FakeDoc(Seq.empty, countOverride = Some(2L))
    val request = requestWith(List(OperatorExportInfo("op-1", "data")))

    val ex = intercept[RuntimeException] {
      service invokePrivate streamCellData(new ByteArrayOutputStream(), request, doc)
    }
    ex.getMessage shouldBe "Could not retrieve row at index 0"
  }

  it should "write the selected cell as UTF-8 bytes" in {
    val out = new ByteArrayOutputStream()
    val doc = new FakeDoc(Seq(tupleOf("café", 1), tupleOf("b", 2)))
    val request = requestWith(List(OperatorExportInfo("op-1", "data")))

    service invokePrivate streamCellData(out, request, doc)

    out.toByteArray shouldBe "café".getBytes(StandardCharsets.UTF_8)
  }

  // -- convertFieldToBytes ----------------------------------------------------

  "convertFieldToBytes" should "pass a byte array through unchanged" in {
    val bytes = Array[Byte](1, 2, 3)
    val result = service invokePrivate convertFieldToBytes(bytes)
    result should be theSameInstanceAs bytes
  }

  it should "encode a string as UTF-8" in {
    val result = service invokePrivate convertFieldToBytes("café")
    result shouldBe "café".getBytes(StandardCharsets.UTF_8)
  }

  it should "fall back to toString for any other type" in {
    val result = service invokePrivate convertFieldToBytes(42)
    result shouldBe "42".getBytes(StandardCharsets.UTF_8)
  }

  // -- streamDocumentAsHTML ---------------------------------------------------

  "streamDocumentAsHTML" should "write the first field of the first row as UTF-8" in {
    val out = new ByteArrayOutputStream()
    val doc = new FakeDoc(Seq(tupleOf("<p>café</p>", 1), tupleOf("<p>ignored</p>", 2)))

    service invokePrivate streamDocumentAsHTML(out, doc)

    out.toByteArray shouldBe "<p>café</p>".getBytes(StandardCharsets.UTF_8)
  }

  it should "throw on an empty document because the first row is unguarded" in {
    val out = new ByteArrayOutputStream()
    a[NoSuchElementException] should be thrownBy {
      service invokePrivate streamDocumentAsHTML(out, new FakeDoc(Seq.empty))
    }
  }

  // -- streamDocumentAsParquetZip ---------------------------------------------

  "streamDocumentAsParquetZip" should "copy the document stream verbatim and close it" in {
    val payload = "fake-parquet-zip-bytes".getBytes(StandardCharsets.UTF_8)
    val source = new TrackingInputStream(payload)
    val out = new ByteArrayOutputStream()

    service invokePrivate streamDocumentAsParquetZip(
      new FakeDoc(Seq.empty, stream = Some(source)),
      out
    )

    out.toByteArray shouldBe payload
    source.closed shouldBe true
  }

  it should "propagate a failure from the underlying document stream" in {
    val failing = new VirtualDocument[Tuple] {
      override def getURI: URI = new URI("file:///stub/export")
      override def clear(): Unit = ()
      override def asInputStream(): InputStream = throw new IllegalStateException("no stream")
    }

    val ex = intercept[IllegalStateException] {
      service invokePrivate streamDocumentAsParquetZip(failing, new ByteArrayOutputStream())
    }
    ex.getMessage shouldBe "no stream"
  }

  // -- streamDocumentAsArrow --------------------------------------------------

  "streamDocumentAsArrow" should "write nothing when the document is empty" in {
    val out = new ByteArrayOutputStream()
    service invokePrivate streamDocumentAsArrow(new FakeDoc(Seq.empty), out)
    out.size shouldBe 0
  }

  it should "round-trip the tuples through an Arrow stream" in {
    val out = new ByteArrayOutputStream()
    val rows = Seq(tupleOf("café", 1), tupleOf("b", 2))
    service invokePrivate streamDocumentAsArrow(new FakeDoc(rows), out)

    val allocator = new RootAllocator()
    val reader = new ArrowFileReader(
      new ByteArrayReadableSeekableByteChannel(out.toByteArray),
      allocator
    )
    try {
      reader.loadNextBatch() shouldBe true
      val root = reader.getVectorSchemaRoot
      root.getRowCount shouldBe 2

      val readBack = (0 until root.getRowCount).map(ArrowUtils.getTexeraTuple(_, root))
      readBack.map(_.getField[String]("name")) shouldBe Seq("café", "b")
      readBack.map(_.getField[Any]("count")) shouldBe Seq(1, 2)
    } finally {
      reader.close()
      allocator.close()
    }
  }

  // -- NonClosingOutputStream -------------------------------------------------

  "NonClosingOutputStream" should "flush but not close the wrapped stream" in {
    val wrapped = new TrackingOutputStream()
    val stream = new NonClosingOutputStream(wrapped)

    stream.write("payload".getBytes(StandardCharsets.UTF_8))
    stream.close()

    utf8(wrapped) shouldBe "payload"
    wrapped.closed shouldBe false
  }
}
