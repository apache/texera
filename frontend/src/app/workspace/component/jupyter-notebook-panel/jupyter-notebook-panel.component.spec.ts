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

import { ComponentFixture, fakeAsync, TestBed, tick } from "@angular/core/testing";
import { JupyterNotebookPanelComponent } from "./jupyter-notebook-panel.component";
import { JupyterPanelService } from "../../service/jupyter-panel/jupyter-panel.service";
import { Subject } from "rxjs";
import { ElementRef } from "@angular/core";
import { By, DomSanitizer } from "@angular/platform-browser";

describe("JupyterNotebookPanelComponent", () => {
  let component: JupyterNotebookPanelComponent;
  let fixture: ComponentFixture<JupyterNotebookPanelComponent>;

  let mockJupyterPanelService: any;
  let bypassSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    mockJupyterPanelService = {
      jupyterNotebookPanelVisible$: new Subject<boolean>(),
      setIframeRef: vi.fn(),
      deleteJupyterNotebook: vi.fn(),
      minimizeJupyterNotebookPanel: vi.fn(),
      getJupyterIframeURLForWorkflow: vi.fn().mockResolvedValue("http://localhost:8888"),
    };

    await TestBed.configureTestingModule({
      imports: [JupyterNotebookPanelComponent],
      providers: [{ provide: JupyterPanelService, useValue: mockJupyterPanelService }],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(JupyterNotebookPanelComponent);
    component = fixture.componentInstance;
    // Spy on the shared DomSanitizer so tests can assert the raw URL was trusted,
    // without matching Angular's serialized SafeResourceUrl string.
    bypassSpy = vi.spyOn(TestBed.inject(DomSanitizer), "bypassSecurityTrustResourceUrl");
    fixture.detectChanges();
  });

  // Destroy the component so its subscriptions complete, and restore spies so a
  // shared spy's call history (e.g. console.error) does not accumulate across
  // tests. Mocks are not auto-reset by the Vitest config.
  afterEach(() => {
    fixture.destroy();
    vi.restoreAllMocks();
  });

  it("should create", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    expect(component).toBeTruthy();
  });

  it("should be hidden by default", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    expect(component.isVisible).toBe(false);
  });

  it("should update visibility when service emits", async () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);

    await fixture.whenStable();
    fixture.detectChanges();

    expect(component.isVisible).toBe(true);
  });

  it("should fetch and sanitize URL when panel becomes visible", async () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);

    await fixture.whenStable();
    fixture.detectChanges();

    expect(mockJupyterPanelService.getJupyterIframeURLForWorkflow).toHaveBeenCalled();
    expect(bypassSpy).toHaveBeenCalledWith("http://localhost:8888");
    expect(component.jupyterUrl).toBe(bypassSpy.mock.results[0].value);
  });

  it("should not render the iframe while visible without a URL", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    component.isVisible = true;

    // Rendering with an unset jupyterUrl must not throw NG0904 (unsafe resource
    // URL); the iframe should simply be absent until a URL is available.
    expect(() => fixture.detectChanges()).not.toThrow();
    expect(fixture.nativeElement.querySelector("iframe")).toBeNull();
  });

  it("should render the iframe once a URL is available", async () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);

    await fixture.whenStable();
    fixture.detectChanges();

    const iframe = fixture.nativeElement.querySelector("iframe");
    expect(iframe).not.toBeNull();
    // Don't match the serialized SafeResourceUrl; assert the binding produced a src.
    expect(iframe.hasAttribute("src")).toBe(true);
  });

  it("should clear jupyterUrl and remove the iframe when hidden", async () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);
    await fixture.whenStable();
    fixture.detectChanges();
    expect(component.jupyterUrl).not.toBeNull();
    expect(fixture.nativeElement.querySelector("iframe")).not.toBeNull();

    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(false);
    await fixture.whenStable();
    fixture.detectChanges();
    expect(component.jupyterUrl).toBeNull();
    expect(fixture.nativeElement.querySelector("iframe")).toBeNull();
  });

  it("should not update jupyterUrl when the iframe URL fetch rejects", async () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    mockJupyterPanelService.getJupyterIframeURLForWorkflow.mockRejectedValueOnce(new Error("network error"));

    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);

    await fixture.whenStable();

    expect(mockJupyterPanelService.getJupyterIframeURLForWorkflow).toHaveBeenCalled();
    expect(component.jupyterUrl).toBeNull();
  });

  it("should keep handling visibility emissions after a failed fetch", async () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    mockJupyterPanelService.getJupyterIframeURLForWorkflow
      .mockRejectedValueOnce(new Error("network error"))
      .mockResolvedValueOnce("http://localhost:9999");

    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);
    await fixture.whenStable();

    mockJupyterPanelService.jupyterNotebookPanelVisible$.next(true);
    await fixture.whenStable();
    fixture.detectChanges();

    expect(mockJupyterPanelService.getJupyterIframeURLForWorkflow).toHaveBeenCalledTimes(2);
    expect(bypassSpy).toHaveBeenCalledWith("http://localhost:9999");
    expect(component.jupyterUrl).toBe(bypassSpy.mock.results[0].value);
  });

  it("should call setIframeRef when iframe exists and visible", fakeAsync(() => {
    component.isVisible = true;

    const mockIframe = document.createElement("iframe");
    component.iframeRef = new ElementRef(mockIframe);

    component.checkIframeRef();
    tick(0);

    expect(mockJupyterPanelService.setIframeRef).toHaveBeenCalledWith(mockIframe);
  }));

  it("should NOT call setIframeRef if not visible", fakeAsync(() => {
    component.isVisible = false;

    const mockIframe = document.createElement("iframe");
    component.iframeRef = new ElementRef(mockIframe);

    component.checkIframeRef();
    tick(0);

    expect(mockJupyterPanelService.setIframeRef).not.toHaveBeenCalled();
  }));

  it("should not log an error when checkIframeRef runs while the panel is hidden", fakeAsync(() => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    component.isVisible = false;

    component.checkIframeRef();
    tick(0);

    expect(errorSpy).not.toHaveBeenCalled();
    expect(mockJupyterPanelService.setIframeRef).not.toHaveBeenCalled();
  }));

  it("should log an error when visible but the iframe ref is missing", fakeAsync(() => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    component.isVisible = true;
    component.iframeRef = undefined as any;

    component.checkIframeRef();
    tick(0);

    expect(errorSpy).toHaveBeenCalledWith("Jupyter Iframe reference not found.");
    expect(mockJupyterPanelService.setIframeRef).not.toHaveBeenCalled();
  }));

  it("should delete notebook via service", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    component.deletePanel();
    expect(mockJupyterPanelService.deleteJupyterNotebook).toHaveBeenCalled();
  });

  it("should minimize panel and update visibility", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    // The real service emits false on minimize; visibility is observable-driven,
    // so the mock must emit for isVisible to update.
    mockJupyterPanelService.minimizeJupyterNotebookPanel = vi.fn(() =>
      mockJupyterPanelService.jupyterNotebookPanelVisible$.next(false)
    );
    component.isVisible = true;

    component.minimizePanel();

    expect(component.isVisible).toBe(false);
    expect(mockJupyterPanelService.minimizeJupyterNotebookPanel).toHaveBeenCalled();
  });

  it("should minimize via the header minimize button click", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    component.isVisible = true;
    fixture.detectChanges();

    const minimizeButton = fixture.debugElement.query(By.css(".minimize-button"));
    expect(minimizeButton).not.toBeNull();
    minimizeButton.triggerEventHandler("click", new MouseEvent("click"));

    expect(mockJupyterPanelService.minimizeJupyterNotebookPanel).toHaveBeenCalled();
  });

  it("should delete via the header delete button popconfirm", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    component.isVisible = true;
    fixture.detectChanges();

    const deleteButton = fixture.debugElement.query(By.css(".delete-button"));
    expect(deleteButton).not.toBeNull();
    // Fire the popconfirm's confirm output, which runs the (nzOnConfirm) binding.
    deleteButton.triggerEventHandler("nzOnConfirm", undefined);

    expect(mockJupyterPanelService.deleteJupyterNotebook).toHaveBeenCalled();
  });

  it("should clean up on destroy", () => {
    vi.spyOn(component, "checkIframeRef").mockImplementation(() => {});
    const nextSpy = vi.spyOn(component["destroy$"] as any, "next");
    const completeSpy = vi.spyOn(component["destroy$"] as any, "complete");

    component.ngOnDestroy();

    expect(nextSpy).toHaveBeenCalled();
    expect(completeSpy).toHaveBeenCalled();
  });
});
