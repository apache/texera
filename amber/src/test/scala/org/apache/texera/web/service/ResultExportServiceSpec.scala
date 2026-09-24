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
import com.github.tototoshi.csv.CSVReader
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
import org.apache.texera.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
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

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  InputStream,
  OutputStream,
  StringReader
}
import java.net.{InetSocketAddress, URI, URL}
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.UUID
import java.util.zip.{ZipException, ZipInputStream}
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
  *    exportOperatorResultAsStream / exportOperatorsAsZip) and the upload
  *    plumbing, which read the execution and version tables. Those run against
  *    MockTexeraDB's embedded Postgres — plus, where an operator needs actual
  *    rows, a real Iceberg result table in the configured catalog — and, for
  *    the upload, against a local stand-in for the file service.
  *
  * Breakage caught: an operator whose result is missing silently producing a
  * truncated download instead of a hard failure or a placeholder ZIP entry; a
  * per-operator failure aborting a whole multi-operator dataset export instead
  * of being collected; one ZIP entry's writer closing the shared archive stream
  * and truncating every following operator's entry; the ZIP writers being keyed
  * to anything other than each operator's own outputType; a warehouse-scoped
  * result slipping past the WarehouseReadGuard read check while the feature is
  * off; the generated file name losing the workflow version,
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
  // Postgres. Tests that only need the guards and the error mapping stop short
  // of opening a result document; tests of the writer dispatch and the ZIP
  // framing store real rows first, as real Iceberg result tables in the
  // configured catalog (the arrangement ExecutionResultServiceSpec uses). A
  // result table's storage key is derived from the URI path and creating one
  // overwrites whatever is already there, so every row-bearing operator id
  // below carries a `rexs-` prefix that is unique in the repository — sbt runs
  // amber suites in parallel inside one JVM.
  // ===========================================================================

  // Fixed, not randomised. MockTexeraDB hands every suite its own UUID-named database
  // (MockTexeraDB.scala:120), so there is no cross-suite key to dodge — and random ids
  // only make a failure message that quotes one impossible to reproduce.
  private val testWorkflowWid = 5001
  private val testUserId = 5002

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
  private def resultUriOf(
      eid: Integer,
      operatorId: String,
      warehouse: Option[String] = None
  ): String =
    VFSURIFactory
      .resultURI(
        VFSURIFactory.createPortBaseURI(
          WorkflowIdentity(testWorkflowWid.longValue()),
          ExecutionIdentity(eid.longValue()),
          GlobalPortIdentity(
            PhysicalOpIdentity(OperatorIdentity(operatorId), "main"),
            PortIdentity(),
            input = false
          ),
          warehouse
        )
      )
      .toString

  /** Creates a real Iceberg result table holding `rows`, and records its URI for `eid`. */
  private def storeResult(eid: Integer, operatorId: String, rows: Seq[Tuple]): Unit = {
    val uri = resultUriOf(eid, operatorId)
    val writer = DocumentFactory
      .createDocument(new URI(uri), schema)
      .writer("rexs")
      .asInstanceOf[BufferedItemWriter[Tuple]]
    writer.open()
    rows.foreach(writer.putOne)
    writer.close()
    insertResultUri(eid, uri)
  }

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

  it should "round-trip CSV special characters through the streamed export" in {
    val execution = insertExecution()
    val special = "comma, \"quote\" and\nnewline 中文"
    storeResult(execution.getEid, "rexs-special", Seq(tupleOf(special, 42)))

    val op = OperatorExportInfo("rexs-special", "csv")
    val (stream, fileName) =
      exportService.exportOperatorResultAsStream(dbRequestWith(List(op)), op)

    fileName.getOrElse(fail("expected a file name")) should fullyMatch regex
      s"""wf-oprexs-special-v${testVersion.getVid}-$timestampPattern\\.csv"""

    val out = new ByteArrayOutputStream()
    stream.write(out)

    // Parse with a real CSV reader: the embedded newline makes raw line
    // comparison meaningless, and the quoting is exactly what is under test.
    CSVReader.open(new StringReader(utf8(out))).all() shouldBe
      List(List("name", "count"), List(special, "42"))
  }

  it should "resolve against the latest execution, not any execution with results" in {
    val older = insertExecution()
    storeResult(older.getEid, "rexs-latest", Seq(tupleOf("old", 1)))
    insertExecution() // newer, and it registered no result

    val op = OperatorExportInfo("rexs-latest", "csv")
    val (stream, fileName) =
      exportService.exportOperatorResultAsStream(dbRequestWith(List(op)), op)

    // The older execution's rows must not leak into the newest execution's export.
    stream shouldBe null
    fileName shouldBe None
  }

  it should "read the newest execution's rows when several executions stored results" in {
    val older = insertExecution()
    storeResult(older.getEid, "rexs-latest2", Seq(tupleOf("old", 1)))
    val newer = insertExecution()
    storeResult(newer.getEid, "rexs-latest2", Seq(tupleOf("new", 2)))

    val op = OperatorExportInfo("rexs-latest2", "csv")
    val (stream, _) =
      exportService.exportOperatorResultAsStream(dbRequestWith(List(op)), op)

    val out = new ByteArrayOutputStream()
    stream.write(out)
    csvLines(out) shouldBe List("name,count", "new,2")
  }

  // The "data" outputType through the public path: a real two-row document, so
  // the index guards run against genuine Iceberg reads rather than a FakeDoc
  // (the FakeDoc rejections are covered in the streamCellData section above).

  private def storeCellFixture(): Unit = {
    val execution = insertExecution()
    storeResult(execution.getEid, "rexs-cell", Seq(tupleOf("a", 1), tupleOf("b", 2)))
  }

  private def streamCell(rowIndex: Int, columnIndex: Int): ByteArrayOutputStream = {
    val op = OperatorExportInfo("rexs-cell", "data")
    val request =
      dbRequestWith(List(op)).copy(rowIndex = rowIndex, columnIndex = columnIndex)
    val (stream, _) = exportService.exportOperatorResultAsStream(request, op)
    val out = new ByteArrayOutputStream()
    stream.write(out)
    out
  }

  it should "stream the last cell when both indexes are at their maxima" in {
    storeCellFixture()

    utf8(streamCell(rowIndex = 1, columnIndex = 1)) shouldBe "2"
  }

  it should "reject a rowIndex equal to the row count with the exact reason" in {
    storeCellFixture()

    val ex = intercept[WebApplicationException] { streamCell(rowIndex = 2, columnIndex = 0) }
    ex.getMessage shouldBe "Invalid rowIndex (2). Total rows: 2"
  }

  it should "reject a columnIndex equal to the field count with the exact reason" in {
    storeCellFixture()

    val ex = intercept[WebApplicationException] { streamCell(rowIndex = 0, columnIndex = 2) }
    ex.getMessage shouldBe "Invalid columnIndex (2). Total columns: 2"
  }

  it should "surface an internal error for a negative rowIndex, which the guard lets through" in {
    storeCellFixture()

    // Characterization, not a contract: the guard only checks `rowIndex >= count`,
    // so -1 reaches the Iceberg reader and fails as its internal seek error
    // instead of the "Invalid rowIndex" message.
    val ex = intercept[RuntimeException] { streamCell(rowIndex = -1, columnIndex = 0) }
    ex.getMessage shouldBe "seek operation should not be called"
  }

  it should "surface an index error for a negative columnIndex, which the guard lets through" in {
    storeCellFixture()

    // Characterization: the guard only checks `columnIndex >= length`, so -1
    // reaches the raw field-array access.
    an[ArrayIndexOutOfBoundsException] should be thrownBy
      streamCell(rowIndex = 0, columnIndex = -1)
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

  private def readZipEntries(bytes: Array[Byte]): List[(String, String)] =
    readZipEntryBytes(bytes).map {
      case (name, content) => name -> new String(content, StandardCharsets.UTF_8)
    }

  // The bytes variant exists for entries whose payload is not text (e.g. Arrow).
  private def readZipEntryBytes(bytes: Array[Byte]): List[(String, Array[Byte])] = {
    val zipIn = new ZipInputStream(new ByteArrayInputStream(bytes))
    try {
      Iterator
        .continually(zipIn.getNextEntry)
        .takeWhile(_ != null)
        .map(entry => entry.getName -> zipIn.readAllBytes())
        .toList
    } finally zipIn.close()
  }

  // -- exportOperatorsAsZip -----------------------------------------------------

  private def writeZipBody(request: ResultExportRequest): Array[Byte] = {
    val (stream, _) = exportService.exportOperatorsAsZip(request)
    val body = new ByteArrayOutputStream()
    stream.write(body)
    body.toByteArray
  }

  "exportOperatorsAsZip" should "throw rather than yield no stream when the workflow never ran" in {
    val request = dbRequestWith(
      List(OperatorExportInfo("op-1", "csv"), OperatorExportInfo("op-2", "csv"))
    )

    // Deliberate asymmetry with exportOperatorResultAsStream, which reports the
    // same condition as (null, None) and leaves the failure to its caller.
    val ex = intercept[WebApplicationException] {
      exportService.exportOperatorsAsZip(request)
    }
    ex.getMessage shouldBe s"No execution result for workflow $testWorkflowWid"
  }

  it should "name the archive after the workflow and write each entry in its operator's own format" in {
    val execution = insertExecution()
    storeResult(execution.getEid, "rexs-csv", Seq(tupleOf("a", 1), tupleOf("b", 2)))
    storeResult(execution.getEid, "rexs-arrow", Seq(tupleOf("c", 3)))
    storeResult(execution.getEid, "rexs-fallback", Seq(tupleOf("d", 4)))

    // The request-level exportType stays "csv": the arrow entry coming out as
    // genuine Arrow pins the dispatch to each operator's own outputType.
    val request = dbRequestWith(
      List(
        OperatorExportInfo("rexs-csv", "csv"),
        OperatorExportInfo("rexs-arrow", "arrow"),
        OperatorExportInfo("rexs-fallback", "not-a-format")
      )
    )

    val (stream, zipName) = exportService.exportOperatorsAsZip(request)
    zipName.getOrElse(fail("expected a zip file name")) should fullyMatch regex
      s"""wf-$timestampPattern\\.zip"""

    val body = new ByteArrayOutputStream()
    stream.write(body)
    val entries = readZipEntryBytes(body.toByteArray)

    val vid = testVersion.getVid
    entries.map(_._1) match {
      case List(csvName, arrowName, fallbackName) =>
        csvName should fullyMatch regex s"""wf-oprexs-csv-v$vid-$timestampPattern\\.csv"""
        arrowName should fullyMatch regex s"""wf-oprexs-arrow-v$vid-$timestampPattern\\.arrow"""
        // The fallback replaces only the writer; the entry keeps the requested extension.
        fallbackName should fullyMatch regex
          s"""wf-oprexs-fallback-v$vid-$timestampPattern\\.not-a-format"""
      case other => fail(s"unexpected entries: $other")
    }

    // In the ZIP path the CSV writer infers its header (no supplied headers).
    new String(entries.head._2, StandardCharsets.UTF_8).linesIterator.toList shouldBe
      List("name,count", "a,1", "b,2")

    val allocator = new RootAllocator()
    val reader = new ArrowFileReader(
      new ByteArrayReadableSeekableByteChannel(entries(1)._2),
      allocator
    )
    try {
      reader.loadNextBatch() shouldBe true
      val root = reader.getVectorSchemaRoot
      root.getRowCount shouldBe 1
      ArrowUtils.getTexeraTuple(0, root).getField[String]("name") shouldBe "c"
    } finally {
      reader.close()
      allocator.close()
    }

    // An unrecognised output type falls back to the CSV writer.
    new String(entries(2)._2, StandardCharsets.UTF_8).linesIterator.toList shouldBe
      List("name,count", "d,4")
  }

  it should "keep the shared ZIP stream open after an entry's writer closes its own stream" in {
    val execution = insertExecution()
    storeResult(execution.getEid, "rexs-first", Seq(tupleOf("a", 1)))
    storeResult(execution.getEid, "rexs-second", Seq(tupleOf("b", 2)))

    val request = dbRequestWith(
      List(OperatorExportInfo("rexs-first", "csv"), OperatorExportInfo("rexs-second", "csv"))
    )

    // CSVWriter.close() closes the stream it was given. Each entry is written
    // through a NonClosingOutputStream so that close cannot end the shared
    // ZipOutputStream; the second entry surviving the first entry's writer is
    // that wrapper actually being routed through, not just existing (its own
    // close-suppression is unit-tested above).
    val entries = readZipEntries(writeZipBody(request))

    entries.map(_._1) should have size 2
    entries.map(_._2.linesIterator.toList) shouldBe List(
      List("name,count", "a,1"),
      List("name,count", "b,2")
    )
  }

  it should "substitute a placeholder for a result-less operator instead of aborting the archive" in {
    val execution = insertExecution()
    storeResult(execution.getEid, "rexs-before", Seq(tupleOf("a", 1)))
    storeResult(execution.getEid, "rexs-after", Seq(tupleOf("b", 2)))
    // A result table that exists but holds no rows: the zero-row half of the
    // guard, distinct from rexs-missing's null-document half.
    storeResult(execution.getEid, "rexs-zero", Seq.empty)

    val request = dbRequestWith(
      List(
        OperatorExportInfo("rexs-before", "csv"),
        OperatorExportInfo("rexs-missing", "csv"),
        OperatorExportInfo("rexs-after", "csv"),
        OperatorExportInfo("rexs-zero", "csv")
      )
    )

    val entries = readZipEntries(writeZipBody(request))

    entries should have size 4
    entries(1) shouldBe ("rexs-missing-empty.txt" -> "Operator rexs-missing has no results")
    entries(3) shouldBe ("rexs-zero-empty.txt" -> "Operator rexs-zero has no results")
    // The operators on both sides of the placeholder keep their real entries.
    entries.head._2.linesIterator.toList shouldBe List("name,count", "a,1")
    entries(2)._2.linesIterator.toList shouldBe List("name,count", "b,2")
  }

  it should "write every row on both sides of the CSV chunk boundary" in {
    val execution = insertExecution()
    // The ZIP CSV writer infers its header from the first row, so a stored count
    // of N leaves N-1 rows for the chunked loop: 10/11/12 stored rows exercise
    // one-under, exactly-one and one-over CHUNK_SIZE (10) in that loop.
    Seq(10, 11, 12).foreach { n =>
      storeResult(execution.getEid, s"rexs-chunk$n", (1 to n).map(i => tupleOf(s"r$i", i)))
    }
    val request = dbRequestWith(
      Seq(10, 11, 12).map(n => OperatorExportInfo(s"rexs-chunk$n", "csv")).toList
    )

    val entries = readZipEntries(writeZipBody(request))

    entries.map(_._2.linesIterator.toList) shouldBe Seq(10, 11, 12).map { n =>
      "name,count" +: (1 to n).map(i => s"r$i,$i").toList
    }
  }

  it should "abort mid-stream when the same operator is requested twice" in {
    val execution = insertExecution()
    storeResult(execution.getEid, "rexs-dup", Seq(tupleOf("a", 1)))

    val request = dbRequestWith(
      List(OperatorExportInfo("rexs-dup", "csv"), OperatorExportInfo("rexs-dup", "csv"))
    )
    val (stream, _) = exportService.exportOperatorsAsZip(request)

    // Both entries generate the same second-granularity file name; align to the
    // start of a second so the two generateFileName calls cannot straddle one.
    // The threshold is deliberately low: it leaves at least 900ms for the first
    // entry to stream and both name lookups to run, at the cost of a sub-second
    // sleep, rather than risking a straddle on a loaded runner.
    val msIntoSecond = System.currentTimeMillis() % 1000
    if (msIntoSecond > 100) Thread.sleep(1000 - msIntoSecond)

    // Characterization, not a contract: the duplicate name is only detected
    // after the first entry has been streamed — past the point exportToLocal
    // commits its 200 — so the client receives a truncated archive.
    val body = new ByteArrayOutputStream()
    val ex = intercept[ZipException] { stream.write(body) }
    ex.getMessage should startWith("duplicate entry")
    body.size should be > 0
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

  it should "report a stored-but-empty result as having nothing to export" in {
    val execution = insertExecution()
    storeResult(execution.getEid, "rexs-ds-zero", Seq.empty)

    val response = exportService.exportToDataset(
      testUser,
      dbRequestWith(List(OperatorExportInfo("rexs-ds-zero", "csv")))
    )

    // The zero-row half of the guard: a result table exists but holds no rows,
    // and must yield the same message as a missing document — not the "Error
    // exporting operator" wrapper that reading an empty table would produce.
    response.getEntity shouldBe ResultExportResponse(
      "error",
      "No results to export for operator OperatorExportInfo(rexs-ds-zero,csv)"
    )
  }

  // The next two tests exercise exportSingleOperatorToDataset's success path, so
  // they store real rows and receive the uploads with the stub file service
  // defined further below (withUploadServer).

  it should "upload each operator's rows to the file service in that operator's own format" in {
    withUploadServer(200) { recorded =>
      val execution = insertExecution()
      storeResult(execution.getEid, "rexs-ds-csv", Seq(tupleOf("a", 1), tupleOf("b", 2)))
      storeResult(execution.getEid, "rexs-ds-fallback", Seq(tupleOf("c", 3)))

      val request = dbRequestWith(
        List(
          OperatorExportInfo("rexs-ds-csv", "csv"),
          OperatorExportInfo("rexs-ds-fallback", "not-a-format")
        ),
        datasetIds = List(7),
        filename = "chosen.csv"
      )

      val response = exportService.exportToDataset(testUser, request)

      response.getEntity shouldBe ResultExportResponse(
        "success",
        "csv export done for operator rexs-ds-csv -> file: chosen.csv\n" +
          "not-a-format export done for operator rexs-ds-fallback -> file: chosen.csv"
      )

      recorded should have size 2
      // Unlike the ZIP path, the dataset path hands the CSV writer the schema's
      // attribute names as an explicit header.
      recorded.head.body.linesIterator.toList shouldBe List("name,count", "a,1", "b,2")
      // An unrecognised output type falls back to the CSV writer here too.
      recorded(1).body.linesIterator.toList shouldBe List("name,count", "c,3")
    }
  }

  it should "report overall success and drop the error lines when only some operators export" in {
    withUploadServer(200) { recorded =>
      val execution = insertExecution()
      storeResult(execution.getEid, "rexs-ds-ok", Seq(tupleOf("a", 1)))

      val request = dbRequestWith(
        List(OperatorExportInfo("rexs-ds-ok", "csv"), OperatorExportInfo("rexs-ds-none", "csv")),
        datasetIds = List(7),
        filename = "f.csv"
      )

      val response = exportService.exportToDataset(testUser, request)

      // The partial-success policy: one success makes the whole response a
      // "success", and the per-operator error lines are dropped, not appended.
      response.getEntity shouldBe ResultExportResponse(
        "success",
        "csv export done for operator rexs-ds-ok -> file: f.csv"
      )
      recorded should have size 1
    }
  }

  // -- getOperatorDocument -------------------------------------------------------

  private val getOperatorDocument =
    PrivateMethod[VirtualDocument[Tuple]](Symbol("getOperatorDocument"))

  "getOperatorDocument" should "return null when the operator stored no result URI" in {
    insertExecution()

    val doc =
      exportService invokePrivate getOperatorDocument("op-1", testComputingUnit.getCuid.intValue())

    doc shouldBe null
  }

  it should "open the stored result document for the operator" in {
    val execution = insertExecution()
    storeResult(execution.getEid, "rexs-doc", Seq(tupleOf("a", 1), tupleOf("b", 2)))

    val doc =
      exportService invokePrivate getOperatorDocument(
        "rexs-doc",
        testComputingUnit.getCuid.intValue()
      )

    doc.getCount shouldBe 2
    doc.get().map(_.getField[String]("name")).toList shouldBe List("a", "b")
  }

  it should "refuse a result stored in a per-user warehouse while the feature is off" in {
    val execution = insertExecution()
    // No table is needed: the WarehouseReadGuard check (#6930) fires on the URI,
    // before any catalog access. warehouseEnabled ships (and runs in CI) as
    // false; WarehouseReadGuardSpec pins both settings of the flag directly.
    insertResultUri(execution.getEid, resultUriOf(execution.getEid, "rexs-wh", Some("byo")))

    val ex = intercept[WarehouseUnavailableException] {
      exportService invokePrivate getOperatorDocument(
        "rexs-wh",
        testComputingUnit.getCuid.intValue()
      )
    }
    ex.getMessage should include("warehouse 'byo'")
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
    // The stub server can only stand in for an endpoint that is plain-HTTP, loopback and
    // carries an explicit port; anything else and the production code would make a real
    // network call to a host we are not serving, while we bind a local port for nothing.
    // So validate the whole shape, not just the path.
    //
    // And deliberately `fail`, not `assume`/`cancel`: the port is the real file-service
    // port, so a cancellation would silently delete the strongest tests in this suite
    // while the build stayed green. A loud, actionable failure is the safer default.
    val loopbackHosts = Set("localhost", "127.0.0.1", "::1", "[::1]")
    val endpointIsServable =
      uploadEndpoint.getProtocol == "http" &&
        loopbackHosts.contains(uploadEndpoint.getHost) &&
        uploadEndpoint.getPort > 0 &&
        uploadEndpoint.getPath == "/api/dataset/did/upload"
    if (!endpointIsServable) {
      fail(
        s"the file-service upload endpoint is overridden to something this suite cannot " +
          s"stand in for ($uploadEndpoint); it must be plain http on a loopback host with an " +
          s"explicit port and the default path. Unset the override and re-run."
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
