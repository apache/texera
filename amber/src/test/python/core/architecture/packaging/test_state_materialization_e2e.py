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
       → real IcebergTableWriter (postgres-backed JdbcCatalog)
       → state document at VFSURIFactory.state_uri(base_uri)
       → InputPortMaterializationReaderRunnable.run()
       → DataElement(StateFrame) on the consumer's input queue

and asserts that a state put through `save_state_to_storage_if_needed`
on the producer side actually arrives at the consumer's queue, with the
same payload.

Marked @integration so the CI runner that has postgres + iceberg
catalog DB provisioned (amber-integration) picks it up via
`pytest -m integration`. Earlier versions of this test substituted a
sqlite-backed SqlCatalog to dodge that infra dependency; that diverged
from the prod catalog code path, so we now exercise the real one.
"""

import os
import tempfile
import threading
import uuid

import pytest

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


@pytest.mark.integration
class TestStateMaterializationE2E:
    @pytest.fixture(autouse=True, scope="class")
    def _init_storage_config(self):
        """Initialize StorageConfig + IcebergCatalogInstance for the real
        postgres-backed catalog in the `amber-integration` CI job.

        Critical detail: the Scala integration tests that run earlier in
        the same job connect to the iceberg catalog DB as user
        `postgres/postgres` (the storage.conf default for
        `STORAGE_ICEBERG_CATALOG_POSTGRES_USERNAME/PASSWORD`). pyiceberg
        creates the catalog's `iceberg_tables` metadata table on first
        use, owned by whoever wrote first — so it ends up owned by
        `postgres`. We MUST connect as the same user, otherwise we hit
        `permission denied for table iceberg_tables`.

        Why the reset: `test_iceberg_document.py` also calls
        `StorageConfig.initialize` at module import time (with a
        different `texera/password` user that works for it because no
        Scala writes first in the `pyamber` job where it runs). pytest
        imports every test module during collection, even ones whose
        tests will be deselected by `-m integration`, so that
        initialization happens here too. We force-reset the singletons
        and re-init with the prod-correct credentials; safe because
        test_iceberg_document's tests are deselected from this run.

        All catalog + S3 settings read the same `STORAGE_*` env vars
        the production code consumes (via storage.conf), so the test
        matches whichever identity the Scala side uses in the same job
        and stays aligned with the bucket / endpoint the workflow
        provisions. Defaults mirror storage.conf so a local sbt run
        without those vars exported still works.

        Class-scoped so the reset + tempdir allocation happens once
        per class; the two tests in this class share state through the
        same StorageConfig singleton anyway.
        """
        StorageConfig._initialized = False
        IcebergCatalogInstance._instance = None
        large_binaries_bucket = os.environ.get(
            "STORAGE_S3_LARGE_BINARIES_BUCKET", "texera-large-binaries"
        )
        StorageConfig.initialize(
            catalog_type="postgres",
            postgres_uri_without_scheme=os.environ.get(
                "STORAGE_ICEBERG_CATALOG_POSTGRES_URI_WITHOUT_SCHEME",
                "localhost:5432/texera_iceberg_catalog",
            ),
            postgres_username=os.environ.get(
                "STORAGE_ICEBERG_CATALOG_POSTGRES_USERNAME", "postgres"
            ),
            postgres_password=os.environ.get(
                "STORAGE_ICEBERG_CATALOG_POSTGRES_PASSWORD", "postgres"
            ),
            rest_catalog_uri="http://localhost:8181/catalog/",
            rest_catalog_warehouse_name="texera",
            table_result_namespace="operator-port-result",
            table_state_namespace="operator-port-state",
            directory_path=tempfile.mkdtemp(prefix="texera-state-e2e-warehouse-"),
            commit_batch_size=4096,
            s3_endpoint=os.environ.get("STORAGE_S3_ENDPOINT", "http://localhost:9000"),
            s3_region=os.environ.get("STORAGE_S3_REGION", "us-west-2"),
            s3_auth_username=os.environ.get("STORAGE_S3_AUTH_USERNAME", "texera_minio"),
            s3_auth_password=os.environ.get("STORAGE_S3_AUTH_PASSWORD", "password"),
            s3_large_binaries_base_uri=f"s3://{large_binaries_bucket}/objects/0/",
        )

    @pytest.fixture
    def base_uri(self) -> str:
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

    @pytest.fixture
    def producer(self, base_uri):
        """An OutputManager wired to the iceberg result + state documents
        at `base_uri`. Closes its writer threads on teardown so cached
        buffers are flushed even if a test errors out before
        `close_port_storage_writers()`.
        """
        # RegionExecutionCoordinator's responsibility in prod: provision
        # result + state documents at the port base URI before any
        # worker starts. We emulate that here.
        DocumentFactory.create_document(
            VFSURIFactory.result_uri(base_uri), State.SCHEMA
        )
        DocumentFactory.create_document(VFSURIFactory.state_uri(base_uri), State.SCHEMA)

        mgr = OutputManager(worker_id="Worker:WF0-test-producer-main-0")
        mgr.add_output_port(
            PortIdentity(id=0, internal=False),
            schema=State.SCHEMA,
            storage_uri_base=base_uri,
        )
        try:
            yield mgr
        finally:
            # close_port_storage_writers is idempotent — fine to call
            # again here if the test already closed.
            try:
                mgr.close_port_storage_writers()
            except Exception:
                pass

    def test_state_written_by_output_manager_is_replayed_by_reader(
        self, base_uri, producer
    ):
        """Producer side writes a state via OutputManager; consumer side
        reads it via InputPortMaterializationReaderRunnable. The state
        must arrive on the consumer's input queue intact.
        """
        # Drive a state through the producer-side path.
        state = State({"flag": True, "loop_counter": 7, "name": "outer"})
        producer.save_state_to_storage_if_needed(state)

        # Force the writer threads to flush + commit by closing them.
        # Without this, the iceberg buffer holds the state in memory
        # and nothing is durable yet.
        producer.close_port_storage_writers()

        # Consumer side: spin up the materialization reader against the
        # same base URI. Each reader needs a partitioning even when no
        # real downstream worker exists — supply a OneToOnePartitioning
        # whose only receiver is the consumer worker itself.
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

        # Run the reader on a worker thread so we can time out cleanly
        # if something goes wrong.
        reader_thread = threading.Thread(target=reader.run, daemon=True)
        reader_thread.start()
        reader_thread.join(timeout=30)
        assert not reader_thread.is_alive(), "reader did not finish within timeout"
        assert reader.finished(), "reader exited but did not mark itself finished"

        # Drain the consumer's queue and find the StateFrame(s).
        state_frames: list[State] = []
        while not consumer_queue.is_empty():
            elem = consumer_queue.get()
            if isinstance(elem, DataElement) and isinstance(elem.payload, StateFrame):
                state_frames.append(elem.payload.frame)

        assert len(state_frames) == 1, (
            f"expected exactly one State to flow through writer→iceberg→reader; "
            f"got {len(state_frames)}: {state_frames}"
        )
        assert state_frames[0] == state, (
            f"replayed state did not match what was written; "
            f"wrote={state}, read={state_frames[0]}"
        )

    def test_state_table_persists_across_writer_close(self, base_uri, producer):
        """Independently verify the iceberg state table contains the row.
        If this passes but the reader test above fails, the bug is in
        the reader / consumer wiring; if this fails, the bug is in the
        writer / storage layer.
        """
        state = State({"flag": False, "checkpoint": 42})
        producer.save_state_to_storage_if_needed(state)
        producer.close_port_storage_writers()

        # Read directly from the iceberg state document, bypassing the
        # reader.
        state_document, _ = DocumentFactory.open_document(
            VFSURIFactory.state_uri(base_uri)
        )
        rows = list(state_document.get())
        assert len(rows) == 1, (
            f"expected exactly one row in the iceberg state table after "
            f"the writer was closed; got {len(rows)} rows"
        )
        assert State.from_tuple(rows[0]) == state
