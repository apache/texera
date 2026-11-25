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
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.storage.result.iceberg.IcebergDocumentTestSupport.{
  allTypesSchema,
  buildAllTypesTuple,
  openTupleDocument,
  uniquePortBaseURI
}
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.exceptions.NoSuchTableException

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.net.URI
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

@IntegrationTest
class IcebergDocumentSpec extends VirtualDocumentSpec[Tuple] {

  var baseURI: URI = _
  var uri: URI = _

  override def beforeEach(): Unit = {
    // Generate a unique table name for each test
    baseURI = uniquePortBaseURI()
    uri = VFSURIFactory.resultURI(baseURI)
    DocumentFactory.createDocument(uri, allTypesSchema)
    super.beforeEach()
  }

  override def getDocument: VirtualDocument[Tuple] = openTupleDocument(uri)

  private def createStateDocument(): VirtualDocument[Tuple] = {
    val stateUri = VFSURIFactory.stateURI(baseURI)
    DocumentFactory.createDocument(stateUri, State.schema)
    openTupleDocument(stateUri)
  }

  it should "not trigger excessive catalog seeks when reading the last file (lazy file advancement)" in {
    val batchSize = StorageConfig.icebergTableCommitBatchSize
    val items = generateSampleItems().take(batchSize * 2)
    val (batch1, batch2) = items.splitAt(batchSize)

    // Write two separate batches to produce two committed data files.
    withWriter(document)(w => batch1.foreach(w.putOne))
    withWriter(document)(w => batch2.foreach(w.putOne))

    val loadCount = new AtomicInteger(0)
    val realCatalog = IcebergCatalogInstance.getInstance()
    IcebergCatalogInstance.replaceInstance(catalogWithLoadSpy(realCatalog, loadCount))
    // Open a fresh reader; it resolves its catalog per use (#7290), so every metadata
    // load inside seekToUsableFile goes through the spy installed above.
    val readerDoc = getDocument
    try {
      val retrieved = readerDoc.get().toList
      assert(
        retrieved.toSet == items.toSet,
        "All records from both files should be read correctly"
      )
      // With lazy file advancement the table is resolved once per seekToUsableFile —
      // the construction seek and the final exhausted-files seek → 2 total. Without
      // lazy advancement it would be once per hasNext() on the last file → O(batchSize).
      assert(
        loadCount.get() <= 4,
        s"the table should be loaded at most 4 times (lazy advancement), but was ${loadCount.get()}"
      )
    } finally {
      IcebergCatalogInstance.replaceInstance(realCatalog)
    }
  }

  it should "report documentExists=true for a URI that was created via createDocument" in {
    assert(DocumentFactory.documentExists(uri))
  }

  it should "report documentExists=false for a URI that was never created" in {
    val freshUri = VFSURIFactory.resultURI(uniquePortBaseURI("fresh"))
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
    val stateDocument = createStateDocument()
    val state = State(
      Map(
        "i" -> 3,
        "name" -> "outer-loop",
        "payload" -> Array[Byte](0, 1, 2, 3),
        "nested" -> Map("enabled" -> true, "values" -> List(1, 2, 3))
      )
    )

    withWriter(stateDocument)(_.putOne(state.toTuple(loopCounter = 7L, loopStartId = "ls")))

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
    val stateDocument = createStateDocument()
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

    withWriter(stateDocument) { writer =>
      states.foreach { case (state, loopCounter) => writer.putOne(state.toTuple(loopCounter)) }
    }

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
    withWriter(document)(w => items.foreach(w.putOne))

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

  it should "report numeric/date table statistics, non-zero file size, and throw NoSuchTableException once cleared" in {
    val items = generateSampleItems().take(3)
    withWriter(document)(w => items.foreach(w.putOne))

    val stats = document.getTableStatistics
    assert(stats("col-int")("min") == -1)
    assert(stats("col-int")("max") == Int.MaxValue)
    assert(stats("col-timestamp")("min").isInstanceOf[String])
    assert(stats("col-timestamp")("max").isInstanceOf[String])
    assert(document.getTotalFileSize > 0L)

    document.clear()
    intercept[NoSuchTableException](document.getURI)
    intercept[NoSuchTableException](document.getTableStatistics)
    intercept[NoSuchTableException](document.getTotalFileSize)
  }

  /** Returns a dynamic proxy for `realCatalog` that counts `loadTable` calls. */
  private def catalogWithLoadSpy(realCatalog: Catalog, counter: AtomicInteger): Catalog =
    Proxy
      .newProxyInstance(
        classOf[Catalog].getClassLoader,
        Array(classOf[Catalog]),
        new InvocationHandler {
          override def invoke(proxy: Object, method: Method, args: Array[Object]): Object = {
            // The reader re-resolves its table per seek (#7290) instead of refreshing a
            // pinned one, so metadata loads now surface as `loadTable` calls here.
            if (method.getName == "loadTable") counter.incrementAndGet()
            if (args == null) method.invoke(realCatalog) else method.invoke(realCatalog, args: _*)
          }
        }
      )
      .asInstanceOf[Catalog]

  override def generateSampleItems(): List[Tuple] = {
    val baseTuples = List(
      buildAllTypesTuple(
        strVal = "Hello World",
        intVal = 42,
        boolVal = true,
        longVal = 12345678901234L,
        doubleVal = 3.14159,
        timestampVal = new Timestamp(System.currentTimeMillis()),
        binaryVal = Array[Byte](0, 1, 2, 3, 4, 5, 6, 7)
      ),
      buildAllTypesTuple(
        strVal = "",
        intVal = -1,
        boolVal = false,
        longVal = -98765432109876L,
        doubleVal = -0.001,
        timestampVal = new Timestamp(0L),
        binaryVal = Array[Byte](127, -128, 0, 64)
      ),
      buildAllTypesTuple(
        strVal = "Special Characters: \n\t\r",
        intVal = Int.MaxValue,
        boolVal = true,
        longVal = Long.MaxValue,
        doubleVal = Double.MaxValue,
        timestampVal = new Timestamp(1234567890L),
        binaryVal = Array[Byte](1, 2, 3, 4, 5)
      )
    )

    def generateRandomBinary(size: Int): Array[Byte] = {
      val array = new Array[Byte](size)
      scala.util.Random.nextBytes(array)
      array
    }

    val additionalTuples = (1 to 20000).map { i =>
      buildAllTypesTuple(
        if (i % 7 == 0) null else s"Generated String $i",
        if (i % 5 == 0) null else i,
        if (i % 6 == 0) null else i % 2 == 0,
        if (i % 4 == 0) null else i.toLong * 1000000L,
        if (i % 3 == 0) null else i * 0.12345,
        if (i % 8 == 0) null else new Timestamp(System.currentTimeMillis() + i * 1000L),
        if (i % 9 == 0) null else generateRandomBinary(10)
      )
    }

    baseTuples ++ additionalTuples
  }
}
