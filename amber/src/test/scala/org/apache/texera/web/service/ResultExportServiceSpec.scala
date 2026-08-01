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
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.auth.JwtAuth
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{OPERATOR_PORT_EXECUTIONS, WORKFLOW_EXECUTIONS}
import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowComputingUnitDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowComputingUnit,
  WorkflowExecutions,
  WorkflowVersion
}
import org.apache.texera.web.model.http.request.result.{OperatorExportInfo, ResultExportRequest}
import org.apache.texera.web.model.http.response.result.ResultExportResponse
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, PrivateMethodTester}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.net.{InetSocketAddress, URI, URL}
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.UUID
import java.util.zip.ZipInputStream
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.{Response, StreamingOutput}
import scala.collection.mutable.ArrayBuffer

/**
  * Unit tests for ResultExportService.
  *
  * Two layers are covered:
  *
  *  - The pure helpers (parseOperators / validateExportRequest) and the private
  *    export-encoding writers. Every writer takes a VirtualDocument parameter,
  *    so an in-spec fake document is enough for those.
  *
  *  - The request-level entry points (exportToLocal / exportToDataset /
  *    exportOperatorResultAsStream) and the upload plumbing, which read the
  *    execution and version tables. Those run against MockTexeraDB's embedded
  *    Postgres and, for the upload, against a local stand-in for the file
  *    service.
  *
  * Breakage caught: an operator whose result is missing silently producing a
  * truncated download instead of a hard failure or a placeholder ZIP entry; a
  * per-operator failure aborting a whole multi-operator dataset export instead
  * of being collected; the generated file name losing the workflow version,
  * the parquet→zip extension mapping, or the path-separator stripping that
  * keeps it a single path segment; and the dataset upload posting to the wrong
  * URL, dropping the URL-encoding of the file path, or omitting the signed
  * bearer token that authenticates the exporting user to the file service.
  */
class ResultExportServiceSpec
    extends AnyFlatSpec
    with Matchers
    with PrivateMethodTester
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

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

  // ===========================================================================
  // Request-level entry points. These read WORKFLOW_EXECUTIONS /
  // OPERATOR_PORT_EXECUTIONS / WORKFLOW_VERSION, so they need the embedded
  // Postgres. They deliberately stop short of opening a result document: that
  // needs a live Iceberg catalog, which is out of reach here. What is covered
  // is everything around it — the guards, the error mapping, the ZIP framing,
  // the file-name construction and the dataset upload.
  // ===========================================================================

  private val testWorkflowWid = 5000 + scala.util.Random.nextInt(1000)
  private val testUserId = 5000 + scala.util.Random.nextInt(1000)

  private var testUser: User = _
  private var testVersion: WorkflowVersion = _
  private var testComputingUnit: WorkflowComputingUnit = _
  private var workflowExecutionsDao: WorkflowExecutionsDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()

    testUser = new User
    testUser.setUid(testUserId)
    testUser.setName("export_user")
    testUser.setEmail("export@example.com")
    testUser.setPassword("password")
    new UserDao(getDSLContext.configuration()).insert(testUser)

    val workflow = new Workflow
    workflow.setWid(testWorkflowWid)
    workflow.setName("wf-" + UUID.randomUUID().toString.substring(0, 8))
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowDao(getDSLContext.configuration()).insert(workflow)

    testVersion = new WorkflowVersion
    testVersion.setWid(testWorkflowWid)
    testVersion.setContent("{}")
    testVersion.setCreationTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowVersionDao(getDSLContext.configuration()).insert(testVersion)

    testComputingUnit = new WorkflowComputingUnit
    testComputingUnit.setUid(testUserId)
    testComputingUnit.setName("export-unit")
    testComputingUnit.setCreationTime(new Timestamp(System.currentTimeMillis()))
    testComputingUnit.setType(WorkflowComputingUnitTypeEnum.local)
    testComputingUnit.setUri("local://test")
    testComputingUnit.setResource("{}")
    new WorkflowComputingUnitDao(getDSLContext.configuration()).insert(testComputingUnit)

    workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())
  }

  // Executions are per-test state: several cases below distinguish "no
  // execution at all" from "an execution with no stored result URI".
  override protected def afterEach(): Unit = {
    val eids = getDSLContext
      .select(WORKFLOW_EXECUTIONS.EID)
      .from(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.VID.eq(testVersion.getVid))
    getDSLContext
      .deleteFrom(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.in(eids))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.VID.eq(testVersion.getVid))
      .execute()
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  // The service under test, bound to the seeded workflow and computing unit so
  // that getLatestExecutionId resolves against the rows this spec inserts.
  private def exportService: ResultExportService =
    new ResultExportService(
      WorkflowIdentity(testWorkflowWid.longValue()),
      computingUnitId = testComputingUnit.getCuid
    )

  private def dbRequestWith(
      operators: List[OperatorExportInfo],
      datasetIds: List[Int] = List.empty,
      filename: String = "",
      workflowName: String = "wf"
  ): ResultExportRequest =
    requestWith(operators).copy(
      workflowId = testWorkflowWid,
      workflowName = workflowName,
      datasetIds = datasetIds,
      filename = filename,
      computingUnitId = testComputingUnit.getCuid
    )

  private def insertExecution(): WorkflowExecutions = {
    val execution = new WorkflowExecutions
    execution.setVid(testVersion.getVid)
    execution.setUid(testUser.getUid)
    execution.setStatus(0.toByte)
    execution.setResult("")
    execution.setLogLocation("")
    execution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName("export-execution")
    execution.setEnvironmentVersion("test-env-1.0")
    execution.setCuid(testComputingUnit.getCuid)
    workflowExecutionsDao.insert(execution)
    execution
  }

  private def insertResultUri(eid: Integer, resultUri: String): Unit =
    getDSLContext
      .insertInto(OPERATOR_PORT_EXECUTIONS)
      .columns(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
        OPERATOR_PORT_EXECUTIONS.RESULT_URI
      )
      .values(eid, "port-" + UUID.randomUUID().toString.substring(0, 8), resultUri)
      .execute()

  // A well-formed external-output result URI, i.e. exactly the shape
  // getResultUriByLogicalPortId decodes and matches against.
  private def resultUriOf(eid: Integer, operatorId: String): String =
    VFSURIFactory
      .resultURI(
        VFSURIFactory.createPortBaseURI(
          WorkflowIdentity(testWorkflowWid.longValue()),
          ExecutionIdentity(eid.longValue()),
          GlobalPortIdentity(
            PhysicalOpIdentity(OperatorIdentity(operatorId), "main"),
            PortIdentity(),
            input = false
          )
        )
      )
      .toString

  private val timestampPattern = """\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"""

  // -- exportOperatorResultAsStream -------------------------------------------

  "exportOperatorResultAsStream" should "yield no stream when the workflow never ran" in {
    val op = OperatorExportInfo("op-1", "csv")
    val (stream, fileName) =
      exportService.exportOperatorResultAsStream(dbRequestWith(List(op)), op)

    stream shouldBe null
    fileName shouldBe None
  }

  it should "yield no stream when no stored result URI belongs to the operator" in {
    val execution = insertExecution()
    // A result URI exists for the execution, but for a *different* logical
    // operator: the lookup must decode it, reject it, and fall through to the
    // null-document guard rather than handing back someone else's result.
    insertResultUri(execution.getEid, resultUriOf(execution.getEid, "other-op"))

    val op = OperatorExportInfo("op-1", "csv")
    val (stream, fileName) =
      exportService.exportOperatorResultAsStream(dbRequestWith(List(op)), op)

    stream shouldBe null
    fileName shouldBe None
  }

  // -- exportToLocal -----------------------------------------------------------

  // The next two tests assert the same message because both guards in
  // `exportOperatorAsStream` raise it. The FIXTURE is what tells them apart: the first
  // seeds no execution (so only the `execIdOpt.isEmpty` guard can fire), the second seeds
  // one (so that guard cannot fire, and reaching the throw proves the null-document guard
  // did). Naming them for the guard they actually hit keeps that honest.

  "exportToLocal" should "fail loudly when the workflow has no execution at all" in {
    val request = dbRequestWith(List(OperatorExportInfo("op-1", "csv")))

    val ex = intercept[RuntimeException] {
      exportService.exportToLocal(request)
    }
    ex.getMessage shouldBe "Failed to export operator"
  }

  it should "fail loudly when the execution exists but the operator stored no result" in {
    // An execution row exists, so `getLatestExecutionId` returns a value and the first
    // guard is out of play; the operator simply never registered a result URI.
    insertExecution()
    val request = dbRequestWith(List(OperatorExportInfo("op-1", "csv")))

    val ex = intercept[RuntimeException] {
      exportService.exportToLocal(request)
    }
    ex.getMessage shouldBe "Failed to export operator"
  }

  it should "reject a multi-operator export when the workflow never ran" in {
    val request = dbRequestWith(
      List(OperatorExportInfo("op-1", "csv"), OperatorExportInfo("op-2", "arrow"))
    )

    val ex = intercept[WebApplicationException] {
      exportService.exportToLocal(request)
    }
    ex.getMessage shouldBe s"No execution result for workflow $testWorkflowWid"
  }

  it should "stream a ZIP with one placeholder entry per result-less operator" in {
    insertExecution()
    val request = dbRequestWith(
      List(OperatorExportInfo("op-1", "csv"), OperatorExportInfo("op-2", "arrow"))
    )

    val response = exportService.exportToLocal(request)

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getMediaType.toString shouldBe "application/zip"
    response.getHeaderString("Content-Disposition") should fullyMatch regex
      s"""attachment; filename="wf-$timestampPattern\\.zip""""

    // Drive the streaming body: the ZIP is only produced when the container
    // writes the entity out.
    val body = new ByteArrayOutputStream()
    response.getEntity.asInstanceOf[StreamingOutput].write(body)

    readZipEntries(body.toByteArray) shouldBe List(
      "op-1-empty.txt" -> "Operator op-1 has no results",
      "op-2-empty.txt" -> "Operator op-2 has no results"
    )
  }

  private def readZipEntries(bytes: Array[Byte]): List[(String, String)] = {
    val zipIn = new ZipInputStream(new ByteArrayInputStream(bytes))
    try {
      Iterator
        .continually(zipIn.getNextEntry)
        .takeWhile(_ != null)
        .map(entry => entry.getName -> new String(zipIn.readAllBytes(), StandardCharsets.UTF_8))
        .toList
    } finally zipIn.close()
  }

  // -- exportToDataset ---------------------------------------------------------

  "exportToDataset" should "report an error when the workflow never ran" in {
    val response =
      exportService.exportToDataset(
        testUser,
        dbRequestWith(List(OperatorExportInfo("op-1", "csv")))
      )

    response.getStatus shouldBe Response.Status.OK.getStatusCode
    response.getEntity shouldBe ResultExportResponse(
      "error",
      s"Workflow $testWorkflowWid has no execution result"
    )
  }

  it should "collect one message per operator rather than stopping at the first" in {
    insertExecution()
    val request = dbRequestWith(
      List(OperatorExportInfo("op-1", "csv"), OperatorExportInfo("op-2", "arrow"))
    )

    val response = exportService.exportToDataset(testUser, request)

    response.getEntity shouldBe ResultExportResponse(
      "error",
      "No results to export for operator OperatorExportInfo(op-1,csv)\n" +
        "No results to export for operator OperatorExportInfo(op-2,arrow)"
    )
  }

  it should "turn a thrown per-operator failure into an error entry and keep going" in {
    val execution = insertExecution()
    // A stored URI the VFS decoder rejects outright, so the lookup throws
    // instead of returning None — the path the per-operator catch exists for.
    insertResultUri(execution.getEid, "mock:///not-a-vfs-uri")

    // TWO operators. The bad URI is stored against the execution rather than a single
    // operator, so both lookups throw — which is exactly the point: the try/catch lives
    // INSIDE the foreach (ResultExportService.scala:105-114), so op-2 must still be
    // attempted after op-1 blows up. Hoist the catch outside the loop and only op-1's
    // line survives, failing the second assertion.
    val response =
      exportService.exportToDataset(
        testUser,
        dbRequestWith(List(OperatorExportInfo("op-1", "csv"), OperatorExportInfo("op-2", "csv")))
      )

    val entity = response.getEntity.asInstanceOf[ResultExportResponse]
    entity.status shouldBe "error"
    val lines = entity.message.split("\n").toList
    lines should have size 2
    // The "Error exporting operator" prefix is what distinguishes "the export threw"
    // from the guard-returned "No results to export" messages asserted above.
    lines.head should startWith("Error exporting operator OperatorExportInfo(op-1,csv): ")
    lines(1) should startWith("Error exporting operator OperatorExportInfo(op-2,csv): ")
  }

  // -- generateFileName --------------------------------------------------------

  private val generateFileName = PrivateMethod[String](Symbol("generateFileName"))

  "generateFileName" should "combine workflow name, operator id, latest version and timestamp" in {
    val name = exportService invokePrivate generateFileName(dbRequestWith(Nil), "sink", "csv")

    name should fullyMatch regex s"""wf-opsink-v${testVersion.getVid}-$timestampPattern\\.csv"""
  }

  it should "give a parquet export a .zip extension because the payload is an archive" in {
    val name = exportService invokePrivate generateFileName(dbRequestWith(Nil), "sink", "parquet")

    name should fullyMatch regex s"""wf-opsink-v${testVersion.getVid}-$timestampPattern\\.zip"""
  }

  it should "strip path separators so the name stays a single path segment" in {
    val request = dbRequestWith(Nil, workflowName = "a/b\\c")

    val name = exportService invokePrivate generateFileName(request, "d/e\\f", "csv")

    name should startWith("abc-opdef-v")
    name should not include "/"
    name should not include "\\"
  }

  // -- saveStreamToDataset / saveToDatasets ------------------------------------

  private val saveStreamToDataset =
    PrivateMethod[(Option[String], Option[String])](Symbol("saveStreamToDataset"))

  private val noopWriter: OutputStream => Unit = _ => ()

  "saveStreamToDataset" should "name the export with the generated file name" in {
    val (success, error) =
      exportService invokePrivate saveStreamToDataset(
        "sink",
        testUser,
        dbRequestWith(Nil),
        "csv",
        noopWriter
      )

    error shouldBe None
    success.getOrElse(fail("expected a success message")) should fullyMatch regex
      s"""csv export done for operator sink -> file: wf-opsink-v${testVersion.getVid}-$timestampPattern\\.csv"""
  }

  it should "prefer the request's filename override over the generated name" in {
    val (success, error) =
      exportService invokePrivate saveStreamToDataset(
        "sink",
        testUser,
        dbRequestWith(Nil, filename = "chosen.csv"),
        "csv",
        noopWriter
      )

    error shouldBe None
    success shouldBe Some("csv export done for operator sink -> file: chosen.csv")
  }

  private case class RecordedUpload(
      method: String,
      path: String,
      rawQuery: String,
      contentType: String,
      authorization: String,
      body: String
  )

  private lazy val uploadEndpoint: URL =
    new URL(ResultExportService.fileServiceUploadOneFileToDatasetEndpoint)

  /**
    * Stands a throw-away HTTP server up on the very host/port the production
    * upload endpoint points at, since that endpoint is fixed at class-load time
    * from the environment and cannot be redirected from a test.
    */
  private def withUploadServer(status: Int)(body: ArrayBuffer[RecordedUpload] => Unit): Unit = {
    // Deliberately `fail`, not `assume`/`cancel`. The port below is the real file-service
    // port, so anyone running the local stack has it bound — and a cancellation would
    // silently delete the strongest tests in this suite while the build stayed green.
    // A loud failure with an actionable message is the safer default.
    if (uploadEndpoint.getPath != "/api/dataset/did/upload") {
      fail(
        s"the file-service upload endpoint is overridden in this environment " +
          s"(path=${uploadEndpoint.getPath}); unset the override to run this suite"
      )
    }
    val recorded = ArrayBuffer.empty[RecordedUpload]
    // Catch Exception, not just IOException: an endpoint override without an explicit port
    // makes getPort return -1, and `new InetSocketAddress(-1)` throws IllegalArgumentException.
    val server =
      try HttpServer.create(new InetSocketAddress(uploadEndpoint.getPort), 0)
      catch {
        case e: Exception =>
          fail(
            s"cannot bind port ${uploadEndpoint.getPort} for the stub upload server: " +
              s"${e.getMessage}. That is the file-service port — stop the local stack and re-run."
          )
      }
    server.createContext(
      "/",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val payload = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
          recorded += RecordedUpload(
            method = exchange.getRequestMethod,
            path = exchange.getRequestURI.getPath,
            rawQuery = exchange.getRequestURI.getRawQuery,
            contentType = exchange.getRequestHeaders.getFirst("Content-Type"),
            authorization = exchange.getRequestHeaders.getFirst("Authorization"),
            body = payload
          )
          exchange.sendResponseHeaders(status, -1)
          exchange.close()
        }
      }
    )
    server.start()
    try body(recorded)
    finally server.stop(0)
  }

  it should "POST the exported bytes to the file service once per dataset" in {
    withUploadServer(200) { recorded =>
      val request = dbRequestWith(
        Nil,
        datasetIds = List(7, 8),
        filename = "my file.csv",
        workflowName = "wf name"
      )

      val (success, error) =
        exportService invokePrivate saveStreamToDataset(
          "sink",
          testUser,
          request,
          "csv",
          (out: OutputStream) => out.write("hello".getBytes(StandardCharsets.UTF_8))
        )

      error shouldBe None
      success shouldBe Some("csv export done for operator sink -> file: my file.csv")

      // The `did` placeholder in the endpoint template is replaced per dataset.
      recorded.map(_.path).toList shouldBe List(
        "/api/dataset/7/upload",
        "/api/dataset/8/upload"
      )
      recorded.foreach { upload =>
        upload.method shouldBe "POST"
        upload.contentType shouldBe "application/octet-stream"
        // Both query values are URL-encoded, so the spaces survive as '+'.
        upload.rawQuery shouldBe "filePath=my+file.csv&message=Export+from+workflow+wf+name"
        upload.body shouldBe "hello"
        // A genuinely signed token for the exporting user, not a placeholder:
        // the consumer verifies the HMAC against the service's own secret.
        upload.authorization should startWith("Bearer ")
        val claims =
          JwtAuth.jwtConsumer.processToClaims(upload.authorization.stripPrefix("Bearer "))
        claims.getSubject shouldBe testUser.getName
        claims.getClaimValue("email") shouldBe testUser.getEmail
      }
    }
  }

  it should "map a rejected upload to an error message naming the dataset" in {
    withUploadServer(500) { recorded =>
      val request = dbRequestWith(Nil, datasetIds = List(9), filename = "f.csv")

      val (success, error) =
        exportService invokePrivate saveStreamToDataset(
          "sink",
          testUser,
          request,
          "csv",
          noopWriter
        )

      recorded should have size 1
      success shouldBe None
      error shouldBe Some(
        "csv export failed for operator sink: " +
          "Error uploading file to dataset 9: " +
          "Failed to upload file. Server responded with: 500"
      )
    }
  }
}
