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
import { PanelService } from "./panel.service";

describe("PanelService", () => {
  let service: PanelService;

  beforeEach(() => {
    TestBed.configureTestingModule({ providers: [PanelService] });
    service = TestBed.inject(PanelService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("resetPanels() emits on resetPanelStream", () => {
    const next = vi.fn();
    service.resetPanelStream.subscribe(next);

    service.resetPanels();

    expect(next).toHaveBeenCalledTimes(1);
  });

  it("closePanels() emits on closePanelStream", () => {
    const next = vi.fn();
    service.closePanelStream.subscribe(next);

    service.closePanels();

    expect(next).toHaveBeenCalledTimes(1);
  });

  it("keeps the reset and close streams independent of each other", () => {
    const onReset = vi.fn();
    const onClose = vi.fn();
    service.resetPanelStream.subscribe(onReset);
    service.closePanelStream.subscribe(onClose);

    service.resetPanels();

    expect(onReset).toHaveBeenCalledTimes(1);
    expect(onClose).not.toHaveBeenCalled();

    service.closePanels();

    expect(onClose).toHaveBeenCalledTimes(1);
    expect(onReset).toHaveBeenCalledTimes(1);
  });

  it("delivers each emission to every current subscriber", () => {
    const first = vi.fn();
    const second = vi.fn();
    service.closePanelStream.subscribe(first);
    service.closePanelStream.subscribe(second);

    service.closePanels();
    service.closePanels();

    expect(first).toHaveBeenCalledTimes(2);
    expect(second).toHaveBeenCalledTimes(2);
  });

  it("does not replay past emissions to a late subscriber but still forwards future ones (Subject semantics)", () => {
    service.resetPanels();

    const late = vi.fn();
    service.resetPanelStream.subscribe(late);

    expect(late).not.toHaveBeenCalled();

    service.resetPanels();

    expect(late).toHaveBeenCalledTimes(1);
  });
});
