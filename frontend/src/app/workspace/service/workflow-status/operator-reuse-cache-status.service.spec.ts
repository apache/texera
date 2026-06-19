/**
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

import { TestBed } from "@angular/core/testing";
import { Subject } from "rxjs";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { OperatorReuseCacheStatusService } from "./operator-reuse-cache-status.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("OperatorCacheStatusService", () => {
  let service: OperatorReuseCacheStatusService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
      imports: [HttpClientTestingModule],
    });
    service = TestBed.inject(OperatorReuseCacheStatusService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});

describe("OperatorCacheStatusService - behavior", () => {
  let service: OperatorReuseCacheStatusService;
  let cacheStatusEvents$: Subject<any>;
  let reuseCacheOpsChanged$: Subject<any>;
  let mockJointUIService: any;
  let mockJointGraphWrapper: any;
  let mockTexeraGraph: any;
  let mockWorkflowActionService: any;
  let mockWorkflowWebsocketService: any;
  const mockMainJointPaper = {};

  beforeEach(() => {
    cacheStatusEvents$ = new Subject<any>();
    reuseCacheOpsChanged$ = new Subject<any>();

    mockJointUIService = {
      changeOperatorReuseCacheStatus: vi.fn(),
    };

    mockJointGraphWrapper = {
      getMainJointPaper: vi.fn().mockReturnValue(mockMainJointPaper),
    };

    mockTexeraGraph = {
      getReuseCacheOperatorsChangedStream: vi.fn().mockReturnValue(reuseCacheOpsChanged$),
      getOperator: vi.fn().mockImplementation((opID: string) => ({ operatorID: opID })),
    };

    mockWorkflowActionService = {
      getTexeraGraph: vi.fn().mockReturnValue(mockTexeraGraph),
      getJointGraphWrapper: vi.fn().mockReturnValue(mockJointGraphWrapper),
    };

    mockWorkflowWebsocketService = {
      subscribeToEvent: vi.fn().mockReturnValue(cacheStatusEvents$),
    };

    TestBed.configureTestingModule({
      providers: [
        OperatorReuseCacheStatusService,
        { provide: JointUIService, useValue: mockJointUIService },
        { provide: WorkflowActionService, useValue: mockWorkflowActionService },
        { provide: WorkflowWebsocketService, useValue: mockWorkflowWebsocketService },
      ],
    });

    service = TestBed.inject(OperatorReuseCacheStatusService);
  });

  it("calls changeOperatorReuseCacheStatus for each operator in a CacheStatusUpdateEvent", () => {
    const event = {
      cacheStatusMap: {
        op1: "cache",
        op2: "no-cache",
      },
    };

    cacheStatusEvents$.next(event);

    expect(mockJointUIService.changeOperatorReuseCacheStatus).toHaveBeenCalledTimes(2);
    expect(mockJointUIService.changeOperatorReuseCacheStatus).toHaveBeenCalledWith(
      mockMainJointPaper,
      { operatorID: "op1" },
      "cache"
    );
    expect(mockJointUIService.changeOperatorReuseCacheStatus).toHaveBeenCalledWith(
      mockMainJointPaper,
      { operatorID: "op2" },
      "no-cache"
    );
  });

  it("does not call changeOperatorReuseCacheStatus when mainJointPaper is null on CacheStatusUpdateEvent", () => {
    mockJointGraphWrapper.getMainJointPaper.mockReturnValue(null);

    cacheStatusEvents$.next({ cacheStatusMap: { op1: "cache" } });

    expect(mockJointUIService.changeOperatorReuseCacheStatus).not.toHaveBeenCalled();
  });

  it("calls changeOperatorReuseCacheStatus for all ops in a reuse cache operators changed event", () => {
    const event = {
      newReuseCacheOps: ["op1", "op2"],
      newUnreuseCacheOps: ["op3"],
    };

    reuseCacheOpsChanged$.next(event);

    expect(mockJointUIService.changeOperatorReuseCacheStatus).toHaveBeenCalledTimes(3);
    expect(mockJointUIService.changeOperatorReuseCacheStatus).toHaveBeenCalledWith(mockMainJointPaper, {
      operatorID: "op1",
    });
    expect(mockJointUIService.changeOperatorReuseCacheStatus).toHaveBeenCalledWith(mockMainJointPaper, {
      operatorID: "op2",
    });
    expect(mockJointUIService.changeOperatorReuseCacheStatus).toHaveBeenCalledWith(mockMainJointPaper, {
      operatorID: "op3",
    });
  });

  it("does not call changeOperatorReuseCacheStatus when mainJointPaper is null on reuse cache changed event", () => {
    mockJointGraphWrapper.getMainJointPaper.mockReturnValue(null);

    reuseCacheOpsChanged$.next({ newReuseCacheOps: ["op1"], newUnreuseCacheOps: [] });

    expect(mockJointUIService.changeOperatorReuseCacheStatus).not.toHaveBeenCalled();
  });

  it("does not throw when reuse cache changed event has empty operator lists", () => {
    expect(() => {
      reuseCacheOpsChanged$.next({ newReuseCacheOps: [], newUnreuseCacheOps: [] });
    }).not.toThrow();
  });
});
