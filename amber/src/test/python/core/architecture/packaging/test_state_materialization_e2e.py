# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
End-to-end integration test for cross-region state materialization.

The unit tests for `OutputManager.save_state_to_storage_if_needed` and
`InputPortMaterializationReaderRunnable.run` mock out the iceberg layer,
so a regression in the writer/storage/reader join is invisible to them.
This test wires:

    OutputManager.set_up_port_storage_writer(port, base_uri)
       → real PortStorageWriter thread
       → real IcebergTableWriter (sqlite-backed SqlCatalog)
       → state document at VFSURIFactory.state_uri(base_uri)
       → InputPortMaterializationReaderRunnable.run()
       → DataElement(StateFrame) on the consumer's input queue

and asserts that a state put through `save_state_to_storage_if_needed`
on the producer side actually arrives at the consumer's queue, with the
same payload.
"""

import tempfile
import threading
import uuid

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from core.architecture.packaging.output_manager import OutputManager
from core.models import State, StateFrame
from core.models.internal_queue import DataElement, InternalQueue
from core.storage.document_factory import DocumentFactory
from core.storage.iceberg.iceberg_catalog_instance import IcebergCatalogInstance
from core.storage.runnables.input_port_materialization_reader_runnable import (
    InputPortMaterializationReaderRunnable,
)
from core.storage.storage_config import StorageConfig
from core.storage.vfs_uri_factory import VFSURIFactory
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    ExecutionIdentity,
    GlobalPortIdentity,
    OperatorIdentity,
    PhysicalOpIdentity,
    PortIdentity,
    WorkflowIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    OneToOnePartitioning,
    Partitioning,
)


# Module-level scratch dir for the sqlite catalog + iceberg warehouse.
# We don't initialize `StorageConfig` here: other test modules (e.g.
# test_iceberg_document.py) also call `StorageConfig.initialize` at
# import time, and the class rejects re-initialization with
# RuntimeError. Whichever module gets collected first wins; we adopt
# its namespaces below.
_WAREHOUSE_DIR = tempfile.mkdtemp(prefix="texera-state-e2e-warehouse-")


@pytest.fixture(scope="module", autouse=True)
def sqlite_iceberg_catalog():
    """Inject a sqlite-backed SqlCatalog so the test runs without external
    iceberg infra (postgres/minio).

    Note: the other iceberg-backed tests (e.g. test_iceberg_document.py) use a
    postgres/REST catalog to mirror production. This e2e deliberately diverges
    to a hermetic sqlite catalog so the writer→storage→reader join can run as a
    fast, infra-free unit test -- the materialization logic it exercises is
    catalog-agnostic, so the sqlite backend exercises the same code path.

    Module-scoped so all tests in this file share one warehouse, and so
    namespace creation only happens once. We save/restore the original
    `IcebergCatalogInstance` singleton so other test modules that expect
    a real postgres-backed catalog (e.g. test_iceberg_document.py) are
    not affected by our replacement.
    """
    # Some other test module may have initialized StorageConfig already
    # (it has a single-init lock). If nothing has initialized it yet,
    # do it here with arbitrary values -- we replace the catalog
    # instance below so the postgres/rest fields are never exercised.
    if not StorageConfig._initialized:
        StorageConfig.initialize(
            catalog_type="postgres",
            postgres_uri_without_scheme="unused",
            postgres_username="unused",
            postgres_password="unused",
            rest_catalog_uri="unused",
            rest_catalog_warehouse_name="unused",
            table_result_namespace="operator-port-result",
            table_state_namespace="operator-port-state",
            directory_path=_WAREHOUSE_DIR,
            commit_batch_size=4096,
            s3_endpoint="unused",
            s3_region="unused",
            s3_auth_username="unused",
            s3_auth_password="unused",
            s3_large_binaries_base_uri="s3://texera-large-binaries/objects/0/",
        )

    original_instance = IcebergCatalogInstance._instance
    db_path = f"{_WAREHOUSE_DIR}/catalog.sqlite"
    catalog = SqlCatalog(
        "texera_iceberg_e2e",
        **{
            "uri": f"sqlite:///{db_path}",
            "warehouse": f"file://{_WAREHOUSE_DIR}",
        },
    )
    # Adopt whatever namespaces StorageConfig already has -- those are
    # the ones DocumentFactory will route into.
    catalog.create_namespace_if_not_exists(StorageConfig.ICEBERG_TABLE_RESULT_NAMESPACE)
    catalog.create_namespace_if_not_exists(StorageConfig.ICEBERG_TABLE_STATE_NAMESPACE)
    IcebergCatalogInstance.replace_instance(catalog)
    try:
        yield catalog
    finally:
        IcebergCatalogInstance.replace_instance(original_instance)


def _fresh_base_uri() -> str:
    """A unique port-base URI per test so tables don't collide."""
    return VFSURIFactory.create_port_base_uri(
        WorkflowIdentity(id=0),
        ExecutionIdentity(id=0),
        GlobalPortIdentity(
            op_id=PhysicalOpIdentity(
                logical_op_id=OperatorIdentity(id=f"e2e-{uuid.uuid4().hex}"),
                layer_name="main",
            ),
            port_id=PortIdentity(id=0, internal=False),
            input=False,
        ),
    )


def test_state_written_by_output_manager_is_replayed_by_reader():
    """Producer side writes a state via OutputManager; consumer side reads
    it via InputPortMaterializationReaderRunnable. The state must arrive
    on the consumer's input queue intact.
    """
    base_uri = _fresh_base_uri()
    port_id = PortIdentity(id=0, internal=False)
    worker_schema_for_result = State.SCHEMA  # producer-side: only state matters

    # 1. RegionExecutionManager's responsibility: provision result +
    # state documents at the port base URI before any worker starts.
    # We emulate that here.
    DocumentFactory.create_document(
        VFSURIFactory.result_uri(base_uri), worker_schema_for_result
    )
    DocumentFactory.create_document(VFSURIFactory.state_uri(base_uri), State.SCHEMA)

    # 2. Producer side: spin up an OutputManager, set up real state +
    # result writer threads against the iceberg storage.
    producer = OutputManager(worker_id="Worker:WF0-test-producer-main-0")
    producer.add_output_port(
        port_id, schema=worker_schema_for_result, storage_uri_base=base_uri
    )

    # 3. Drive a state through the producer-side path. The loop bookkeeping
    # rides alongside the State (not inside it) and is materialized as its own
    # set of columns. Use non-default values for both so a regression in
    # either column's plumbing is caught, not just loop_counter's.
    state = State({"flag": True, "name": "outer"})
    producer.save_state_to_storage_if_needed(
        state,
        loop_counter=7,
        loop_start_id="outer-loop",
    )

    # 4. Force the writer threads to flush + commit by closing them.
    # Without this, the iceberg buffer holds the state in memory and
    # nothing is durable yet.
    producer.close_port_storage_writers()

    # 5. Consumer side: spin up the materialization reader against the
    # same base URI. Each reader needs a partitioning even when no real
    # downstream worker exists -- supply a OneToOnePartitioning whose
    # only receiver is the consumer worker itself.
    consumer_worker = ActorVirtualIdentity(name="consumer-worker-0")
    consumer_queue = InternalQueue()
    partitioning = Partitioning(
        one_to_one_partitioning=OneToOnePartitioning(
            batch_size=400,
            channels=[
                ChannelIdentity(
                    from_worker_id=ActorVirtualIdentity(name="producer-worker-0"),
                    to_worker_id=consumer_worker,
                    is_control=False,
                )
            ],
        )
    )
    reader = InputPortMaterializationReaderRunnable(
        uri=base_uri,
        queue=consumer_queue,
        worker_actor_id=consumer_worker,
        partitioning=partitioning,
    )

    # Run the reader on a worker thread so we can time out cleanly if
    # something goes wrong.
    reader_thread = threading.Thread(target=reader.run, daemon=True)
    reader_thread.start()
    reader_thread.join(timeout=30)
    assert not reader_thread.is_alive(), "reader did not finish within timeout"
    assert reader.finished(), "reader exited but did not mark itself finished"

    # 6. Drain the consumer's queue and find the StateFrame(s).
    state_frames: list[StateFrame] = []
    while not consumer_queue.is_empty():
        elem = consumer_queue.get()
        if isinstance(elem, DataElement) and isinstance(elem.payload, StateFrame):
            state_frames.append(elem.payload)

    assert len(state_frames) == 1, (
        f"expected exactly one State to flow through writer→iceberg→reader; "
        f"got {len(state_frames)}: {state_frames}"
    )
    assert state_frames[0].frame == state, (
        f"replayed state did not match what was written; "
        f"wrote={state}, read={state_frames[0].frame}"
    )
    assert state_frames[0].loop_counter == 7, (
        f"loop_counter must round-trip through its own column; "
        f"got {state_frames[0].loop_counter}"
    )
    assert state_frames[0].loop_start_id == "outer-loop", (
        f"loop_start_id must round-trip through its own column; "
        f"got {state_frames[0].loop_start_id!r}"
    )


def test_state_table_persists_across_writer_close():
    """Independently verify the iceberg state table contains the row.
    If this passes but the reader test above fails, the bug is in the
    reader / consumer wiring; if this fails, the bug is in the writer /
    storage layer.
    """
    base_uri = _fresh_base_uri()
    port_id = PortIdentity(id=0, internal=False)

    DocumentFactory.create_document(VFSURIFactory.result_uri(base_uri), State.SCHEMA)
    DocumentFactory.create_document(VFSURIFactory.state_uri(base_uri), State.SCHEMA)

    producer = OutputManager(worker_id="Worker:WF0-test-producer2-main-0")
    producer.add_output_port(port_id, schema=State.SCHEMA, storage_uri_base=base_uri)

    state = State({"flag": False, "checkpoint": 42})
    producer.save_state_to_storage_if_needed(
        state,
        loop_counter=3,
        loop_start_id="inner-loop",
    )
    producer.close_port_storage_writers()

    # Read directly from the iceberg state document, bypassing the reader.
    state_document, _ = DocumentFactory.open_document(VFSURIFactory.state_uri(base_uri))
    rows = list(state_document.get())
    assert len(rows) == 1, (
        f"expected exactly one row in the iceberg state table after the "
        f"writer was closed; got {len(rows)} rows"
    )
    assert State.from_tuple(rows[0]) == state
    assert rows[0][State.LOOP_COUNTER] == 3
    assert rows[0][State.LOOP_START_ID] == "inner-loop"
