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

package org.apache.texera.amber.storage.result.iceberg

import org.apache.texera.common.config.StorageConfig
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.storage.model.{VirtualDocument, VirtualDocumentSpec}
import org.apache.texera.amber.core.storage.{DocumentFactory, IcebergCatalogInstance, VFSURIFactory}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.iceberg.Table
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.amber.util.IcebergUtil
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.Record
import org.apache.iceberg.{Schema => IcebergSchema}
import org.scalatest.BeforeAndAfterAll

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.net.URI
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

@IntegrationTest
class IcebergDocumentSpec extends VirtualDocumentSpec[Tuple] with BeforeAndAfterAll {

  var amberSchema: Schema = _
  var icebergSchema: IcebergSchema = _
  var serde: (IcebergSchema, Tuple) => Record = _
  var deserde: (IcebergSchema, Record) => Tuple = _
  var catalog: Catalog = _
  val tableNamespace = "test_namespace"
  var baseURI: URI = _
  var uri: URI = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Initialize Amber Schema with all possible attribute types
    amberSchema = Schema(
      List(
        new Attribute("col-string", AttributeType.STRING),
        new Attribute("col-int", AttributeType.INTEGER),
        new Attribute("col-bool", AttributeType.BOOLEAN),
        new Attribute("col-long", AttributeType.LONG),
        new Attribute("col-double", AttributeType.DOUBLE),
        new Attribute("col-timestamp", AttributeType.TIMESTAMP),
        new Attribute("col-binary", AttributeType.BINARY)
      )
    )

    // Initialize Iceberg Schema
    icebergSchema = IcebergUtil.toIcebergSchema(amberSchema)

    // Initialize serialization and deserialization functions
    serde = IcebergUtil.toGenericRecord
    deserde = (schema, record) => IcebergUtil.fromRecord(record, amberSchema)
  }

  override def beforeEach(): Unit = {
    // Generate a unique table name for each test
    baseURI = VFSURIFactory.createPortBaseURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      GlobalPortIdentity(
        PhysicalOpIdentity(
          logicalOpId =
            OperatorIdentity(s"test-table-${UUID.randomUUID().toString.replace("-", "")}"),
          layerName = "main"
        ),
        PortIdentity()
      )
    )
    uri = VFSURIFactory.resultURI(baseURI)
    DocumentFactory.createDocument(uri, amberSchema)
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def getDocument: VirtualDocument[Tuple] = {
    DocumentFactory.openDocument(uri)._1.asInstanceOf[VirtualDocument[Tuple]]
  }

  it should "not trigger excessive catalog seeks when reading the last file (lazy file advancement)" in {
    val batchSize = StorageConfig.icebergTableCommitBatchSize
    val items = generateSampleItems().take(batchSize * 2)
    val (batch1, batch2) = items.splitAt(batchSize)

    // Write two separate batches to produce two committed data files.
    // This also initialises `document.catalog` (lazy val) with the real catalog, which
    // is why we open a fresh reader document below after injecting the spy.
    val writer1 = document.writer(UUID.randomUUID().toString)
    writer1.open(); batch1.foreach(writer1.putOne); writer1.close()

    val writer2 = document.writer(UUID.randomUUID().toString)
    writer2.open(); batch2.foreach(writer2.putOne); writer2.close()

    val refreshCount = new AtomicInteger(0)
    val realCatalog = IcebergCatalogInstance.getInstance()
    IcebergCatalogInstance.replaceInstance(catalogWithRefreshSpy(realCatalog, refreshCount))
    // Open a fresh reader: its `catalog` lazy val hasn't been initialised yet, so it
    // will pick up the spy catalog on first access inside seekToUsableFile.
    val readerDoc = getDocument
    try {
      val retrieved = readerDoc.get().toList
      assert(
        retrieved.toSet == items.toSet,
        "All records from both files should be read correctly"
      )
      // With lazy file advancement seekToUsableFile() (and therefore table.refresh()) is called:
      //   once on iterator creation, once when the last file is exhausted → 2 total.
      // Without the fix it would be called once per hasNext() on the last file → O(batchSize).
      assert(
        refreshCount.get() <= 4,
        s"table.refresh() should be called at most 4 times (lazy advancement), but was ${refreshCount.get()}"
      )
    } finally {
      IcebergCatalogInstance.replaceInstance(realCatalog)
    }
  }

  it should "report documentExists=true for a URI that was created via createDocument" in {
    assert(DocumentFactory.documentExists(uri))
  }

  it should "report documentExists=false for a URI that was never created" in {
    val freshBase = VFSURIFactory.createPortBaseURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      GlobalPortIdentity(
        PhysicalOpIdentity(
          logicalOpId = OperatorIdentity(s"fresh-${UUID.randomUUID().toString.replace("-", "")}"),
          layerName = "main"
        ),
        PortIdentity()
      )
    )
    val freshUri = VFSURIFactory.resultURI(freshBase)
    assert(!DocumentFactory.documentExists(freshUri))
  }

  it should "throw UnsupportedOperationException for documentExists on an unsupported scheme" in {
    intercept[UnsupportedOperationException] {
      DocumentFactory.documentExists(new URI("file:///tmp/anything"))
    }
  }

  it should "resolve CONSOLE_MESSAGES URIs through documentExists" in {
    val consoleUri = VFSURIFactory.createConsoleMessagesURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OperatorIdentity(s"fresh-${UUID.randomUUID().toString.replace("-", "")}")
    )
    assert(!DocumentFactory.documentExists(consoleUri))
  }

  it should "resolve RUNTIME_STATISTICS URIs through documentExists" in {
    val statsUri = VFSURIFactory.createRuntimeStatisticsURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0)
    )
    assert(!DocumentFactory.documentExists(statsUri))
  }

  it should "throw IllegalArgumentException for resolveNamespace on an unmapped resource type" in {
    // `resolveNamespace` is private and its `case _ =>` is unreachable from any
    // well-formed VFS URI (VFSURIFactory.decodeURI validates resource types).
    // Exercise the defensive branch by reflecting on the method and passing
    // null — Scala pattern matches fall through to the wildcard for null
    // scrutinees.
    val method = DocumentFactory.getClass.getDeclaredMethod(
      "resolveNamespace",
      classOf[Enumeration#Value]
    )
    method.setAccessible(true)
    val wrapped = intercept[InvocationTargetException] {
      method.invoke(DocumentFactory, null)
    }
    assert(wrapped.getCause.isInstanceOf[IllegalArgumentException])
  }

  it should "round trip materialized state documents" in {
    val stateUri = VFSURIFactory.stateURI(baseURI)
    DocumentFactory.createDocument(stateUri, State.schema)
    val stateDocument =
      DocumentFactory.openDocument(stateUri)._1.asInstanceOf[VirtualDocument[Tuple]]
    val state = State(
      Map(
        "i" -> 3,
        "name" -> "outer-loop",
        "payload" -> Array[Byte](0, 1, 2, 3),
        "nested" -> Map("enabled" -> true, "values" -> List(1, 2, 3))
      )
    )

    val writer = stateDocument.writer(UUID.randomUUID().toString)
    writer.open()
    writer.putOne(state.toTuple(loopCounter = 7L, loopStartId = "ls"))
    writer.close()

    val storedRows = stateDocument.get().toList
    assert(storedRows.length == 1)
    // Loop bookkeeping is materialized as its own columns, not in the content JSON.
    assert(storedRows.head.getField[java.lang.Long]("loop_counter").toLong == 7L)
    assert(storedRows.head.getField[String]("loop_start_id") == "ls")
    // User state round-trips through the content column (fromTuple reads only content).
    val deserialized = State.fromTuple(storedRows.head).values
    assert(deserialized("i") == 3L)
    assert(deserialized("name") == "outer-loop")
    assert(deserialized("payload").asInstanceOf[Array[Byte]].sameElements(Array[Byte](0, 1, 2, 3)))
    assert(deserialized("nested").asInstanceOf[Map[String, Any]]("enabled") == true)
    assert(deserialized("nested").asInstanceOf[Map[String, Any]]("values") == List(1L, 2L, 3L))
  }

  it should "materialize multiple states as rows in one state table" in {
    val stateUri = VFSURIFactory.stateURI(baseURI)
    DocumentFactory.createDocument(stateUri, State.schema)
    val stateDocument =
      DocumentFactory.openDocument(stateUri)._1.asInstanceOf[VirtualDocument[Tuple]]
    // (user state, loopCounter) -- the counter is written to its own column.
    val states: List[(State, Long)] = List(
      (State(Map("i" -> 1, "payload" -> Array[Byte](1, 2, 3))), 0L),
      (
        State(
          Map(
            "i" -> 2,
            "payload" -> Array[Byte](4, 5, 6),
            "nested" -> Map("values" -> List(3, 4))
          )
        ),
        1L
      )
    )

    val writer = stateDocument.writer(UUID.randomUUID().toString)
    writer.open()
    states.foreach { case (state, loopCounter) => writer.putOne(state.toTuple(loopCounter)) }
    writer.close()

    val storedRows =
      stateDocument.get().toList.sortBy(_.getField[java.lang.Long]("loop_counter").toLong)
    assert(storedRows.length == states.length)
    storedRows.zip(states).foreach {
      case (row, (expectedState, expectedLoopCounter)) =>
        // loop_counter is its own column...
        assert(row.getField[java.lang.Long]("loop_counter").toLong == expectedLoopCounter)
        // ...and the user state round-trips through the content column.
        val actual = State.fromTuple(row).values
        assert(actual("i") == expectedState.values("i").asInstanceOf[Int].toLong)
        assert(
          actual("payload")
            .asInstanceOf[Array[Byte]]
            .sameElements(expectedState.values("payload").asInstanceOf[Array[Byte]])
        )
    }
    assert(
      State
        .fromTuple(storedRows(1))
        .values("nested")
        .asInstanceOf[Map[String, Any]]("values") == List(3L, 4L)
    )
  }

  it should "expose written rows as a non-empty ZIP via asInputStream" in {
    val items = generateSampleItems().take(3)
    val writer = document.writer(UUID.randomUUID().toString)
    writer.open()
    items.foreach(writer.putOne)
    writer.close()

    val stream = document.asInputStream()
    try {
      val bytes = stream.readAllBytes()
      assert(bytes.nonEmpty, "asInputStream should yield non-empty bytes after writes")
      // ZIP local-file-header magic bytes: 0x50 0x4B 0x03 0x04 ("PK\x03\x04").
      assert(
        bytes(0) == 0x50.toByte && bytes(1) == 0x4b.toByte,
        "expected ZIP magic bytes at the start of the stream"
      )
    } finally {
      stream.close()
    }
  }

  /** Returns a dynamic proxy for `realTable` that increments `counter` on every `refresh()` call. */
  private def tableWithRefreshSpy(realTable: Table, counter: AtomicInteger): Table =
    Proxy
      .newProxyInstance(
        classOf[Table].getClassLoader,
        Array(classOf[Table]),
        new InvocationHandler {
          override def invoke(proxy: Object, method: Method, args: Array[Object]): Object = {
            if (method.getName == "refresh") counter.incrementAndGet()
            if (args == null) method.invoke(realTable) else method.invoke(realTable, args: _*)
          }
        }
      )
      .asInstanceOf[Table]

  /** Returns a dynamic proxy for `realCatalog` that wraps every loaded `Table` with a refresh spy. */
  private def catalogWithRefreshSpy(realCatalog: Catalog, counter: AtomicInteger): Catalog =
    Proxy
      .newProxyInstance(
        classOf[Catalog].getClassLoader,
        Array(classOf[Catalog]),
        new InvocationHandler {
          override def invoke(proxy: Object, method: Method, args: Array[Object]): Object = {
            val result =
              if (args == null) method.invoke(realCatalog) else method.invoke(realCatalog, args: _*)
            if (method.getName == "loadTable" && result != null)
              tableWithRefreshSpy(result.asInstanceOf[Table], counter)
            else
              result
          }
        }
      )
      .asInstanceOf[Catalog]

  override def generateSampleItems(): List[Tuple] = {
    val baseTuples = List(
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, "Hello World")
        .add("col-int", AttributeType.INTEGER, 42)
        .add("col-bool", AttributeType.BOOLEAN, true)
        .add("col-long", AttributeType.LONG, 12345678901234L)
        .add("col-double", AttributeType.DOUBLE, 3.14159)
        .add("col-timestamp", AttributeType.TIMESTAMP, new Timestamp(System.currentTimeMillis()))
        .add("col-binary", AttributeType.BINARY, Array[Byte](0, 1, 2, 3, 4, 5, 6, 7))
        .build(),
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, "")
        .add("col-int", AttributeType.INTEGER, -1)
        .add("col-bool", AttributeType.BOOLEAN, false)
        .add("col-long", AttributeType.LONG, -98765432109876L)
        .add("col-double", AttributeType.DOUBLE, -0.001)
        .add("col-timestamp", AttributeType.TIMESTAMP, new Timestamp(0L))
        .add("col-binary", AttributeType.BINARY, Array[Byte](127, -128, 0, 64))
        .build(),
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, "Special Characters: \n\t\r")
        .add("col-int", AttributeType.INTEGER, Int.MaxValue)
        .add("col-bool", AttributeType.BOOLEAN, true)
        .add("col-long", AttributeType.LONG, Long.MaxValue)
        .add("col-double", AttributeType.DOUBLE, Double.MaxValue)
        .add("col-timestamp", AttributeType.TIMESTAMP, new Timestamp(1234567890L))
        .add("col-binary", AttributeType.BINARY, Array[Byte](1, 2, 3, 4, 5))
        .build()
    )

    def generateRandomBinary(size: Int): Array[Byte] = {
      val array = new Array[Byte](size)
      scala.util.Random.nextBytes(array)
      array
    }

    val additionalTuples = (1 to 20000).map { i =>
      Tuple
        .builder(amberSchema)
        .add("col-string", AttributeType.STRING, if (i % 7 == 0) null else s"Generated String $i")
        .add("col-int", AttributeType.INTEGER, if (i % 5 == 0) null else i)
        .add("col-bool", AttributeType.BOOLEAN, if (i % 6 == 0) null else i % 2 == 0)
        .add("col-long", AttributeType.LONG, if (i % 4 == 0) null else i.toLong * 1000000L)
        .add("col-double", AttributeType.DOUBLE, if (i % 3 == 0) null else i * 0.12345)
        .add(
          "col-timestamp",
          AttributeType.TIMESTAMP,
          if (i % 8 == 0) null
          else new Timestamp(System.currentTimeMillis() + i * 1000L)
        )
        .add("col-binary", AttributeType.BINARY, if (i % 9 == 0) null else generateRandomBinary(10))
        .build()
    }

    baseTuples ++ additionalTuples
  }
}
