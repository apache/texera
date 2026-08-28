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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { of, Subject } from "rxjs";
import { NzUploadFile } from "ng-zorro-antd/upload";

import { NotebookImportModalComponent } from "./notebook-import-modal.component";
import { NotebookMigrationService } from "../../service/notebook-migration/notebook-migration.service";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("NotebookImportModalComponent", () => {
  let fixture: ComponentFixture<NotebookImportModalComponent>;
  let component: NotebookImportModalComponent;
  let notebookMigrationService: NotebookMigrationService;
  let modalRef: { close: ReturnType<typeof vi.fn>; updateConfig: ReturnType<typeof vi.fn> };
  // The opener-supplied gate; tests set its resolved value to drive close vs stay-open.
  let requestImport: ReturnType<typeof vi.fn>;

  // Configures the modal with the given models$ stream, then creates and renders it.
  async function createWith(models$: unknown): Promise<void> {
    await TestBed.configureTestingModule({
      imports: [NotebookImportModalComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [
        { provide: NzModalRef, useValue: modalRef },
        { provide: NZ_MODAL_DATA, useValue: { requestImport } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    notebookMigrationService = TestBed.inject(NotebookMigrationService);
    vi.spyOn(notebookMigrationService, "getAvailableModels").mockReturnValue(models$ as any);

    fixture = TestBed.createComponent(NotebookImportModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }

  beforeEach(() => {
    modalRef = { close: vi.fn(), updateConfig: vi.fn() };
    requestImport = vi.fn().mockResolvedValue(true);
  });

  it("renders the diagram and a usable model select once models load", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const root = fixture.nativeElement as HTMLElement;
    expect(root.querySelector("img[alt='Notebook to Workflow']")).not.toBeNull();
    expect(root.querySelector("nz-select")).not.toBeNull();
    expect(root.textContent).toContain("Select a model");
  });

  it("shows the loading spinner only while a submission is in flight", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const spinning = () => (fixture.nativeElement as HTMLElement).querySelector(".ant-spin-spinning") !== null;
    expect(spinning()).toBe(false);

    component.isSubmitting = true;
    fixture.detectChanges();
    expect(spinning()).toBe(true);
  });

  it("makes the form and footer inert and announces the overlay while submitting", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const root = fixture.nativeElement as HTMLElement;
    const form = () => root.querySelector(".import-modal-form");
    const footer = () => root.querySelector(".import-modal-footer");

    expect(form()?.hasAttribute("inert")).toBe(false);
    expect(footer()?.hasAttribute("inert")).toBe(false);

    component.isSubmitting = true;
    fixture.detectChanges();

    // While generating, the covered form and footer are pulled out of the focus/a11y tree,
    // and the overlay is a live region so its status is announced.
    expect(form()?.hasAttribute("inert")).toBe(true);
    expect(footer()?.hasAttribute("inert")).toBe(true);
    expect(root.querySelector(".import-modal-loading")?.getAttribute("role")).toBe("status");
  });

  it("shows the disabled 'no models available' select when the list is empty", async () => {
    await createWith(of([]));
    expect((fixture.nativeElement as HTMLElement).textContent).toContain("No models available");
  });

  it("shows the loading select while models have not resolved yet", async () => {
    // A subject that never emits keeps the async pipe pending, so the loading branch renders.
    await createWith(new Subject());
    expect((fixture.nativeElement as HTMLElement).textContent).toContain("Loading models...");
  });

  it("shows the selected file name once a file is on the form", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    component.importForm.patchValue({ file: { name: "demo.ipynb" } });
    fixture.detectChanges();
    expect((fixture.nativeElement as HTMLElement).textContent).toContain("Selected file: demo.ipynb");
  });

  it("beforeUpload stores the file on the form and prevents auto-upload", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const file = { name: "x.ipynb" } as NzUploadFile;

    const result = component.beforeUpload(file);

    expect(result).toBe(false);
    expect(component.importForm.get("file")?.value).toBe(file);
  });

  it("Submit is disabled until the form has both a file and a model", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const submit = () =>
      (fixture.nativeElement as HTMLElement).querySelector(
        ".import-modal-footer button[nzType='primary']"
      ) as HTMLButtonElement;

    // Empty form -> disabled.
    expect(submit().disabled).toBe(true);

    component.importForm.setValue({ file: { name: "x.ipynb" }, model: "gpt-4" });
    fixture.detectChanges();
    expect(submit().disabled).toBe(false);
  });

  it("onSubmit asks the opener to import and closes when it proceeds", async () => {
    requestImport.mockResolvedValue(true);
    await createWith(of([{ name: "gpt-4" }]));
    const file = { name: "x.ipynb" } as NzUploadFile;
    component.importForm.setValue({ file, model: "gpt-4" });

    await component.onSubmit();

    expect(requestImport).toHaveBeenCalledWith(file, "gpt-4");
    expect(modalRef.close).toHaveBeenCalled();
  });

  it("onSubmit keeps the modal open when the opener declines", async () => {
    // e.g. generation failed and the user can retry.
    requestImport.mockResolvedValue(false);
    await createWith(of([{ name: "gpt-4" }]));
    component.importForm.setValue({ file: { name: "x.ipynb" } as NzUploadFile, model: "gpt-4" });

    await component.onSubmit();

    expect(requestImport).toHaveBeenCalled();
    expect(modalRef.close).not.toHaveBeenCalled();
  });

  it("locks the modal shut while generating and restores the close controls on failure", async () => {
    let resolveRequest!: (proceed: boolean) => void;
    requestImport.mockReturnValue(new Promise<boolean>(resolve => (resolveRequest = resolve)));
    await createWith(of([{ name: "gpt-4" }]));
    component.importForm.setValue({ file: { name: "x.ipynb" } as NzUploadFile, model: "gpt-4" });

    const submitting = component.onSubmit();
    // While generation is pending, the X, mask click, and ESC are disabled.
    expect(modalRef.updateConfig).toHaveBeenCalledWith({
      nzClosable: false,
      nzMaskClosable: false,
      nzKeyboard: false,
    });

    resolveRequest(false); // generation failed
    await submitting;
    // The modal stayed open, so the close controls are restored.
    expect(modalRef.updateConfig).toHaveBeenLastCalledWith({
      nzClosable: true,
      nzMaskClosable: true,
      nzKeyboard: true,
    });
    expect(modalRef.close).not.toHaveBeenCalled();
  });

  it("computes the elapsed time from the start timestamp as mm:ss", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    // No generation started yet -> no start timestamp -> zero.
    expect(component.formattedElapsedTime).toBe("0:00");
    (component as any).startTime = 1000;
    vi.spyOn(Date, "now").mockReturnValue(1000 + 62_000);
    expect(component.formattedElapsedTime).toBe("1:02");
  });

  it("runs the stopwatch off wall-clock time while generating and stops the interval when done", async () => {
    let resolveRequest!: (proceed: boolean) => void;
    requestImport.mockReturnValue(new Promise<boolean>(resolve => (resolveRequest = resolve)));
    await createWith(of([{ name: "gpt-4" }]));
    component.importForm.setValue({ file: { name: "x.ipynb" } as NzUploadFile, model: "gpt-4" });

    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date(0));
      const submitting = component.onSubmit();
      expect(component.formattedElapsedTime).toBe("0:00");

      // Advancing the clock (even if the interval were throttled) yields the correct elapsed time.
      vi.advanceTimersByTime(75_000);
      expect(component.formattedElapsedTime).toBe("1:15");

      resolveRequest(false);
      await submitting;
      expect((component as any).timerHandle).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it("has a visibilitychange handler that is safe to call (repaint is driven by the zone event)", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    expect(() => component.onVisibilityChange()).not.toThrow();
  });

  it("clears the stopwatch interval on destroy", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const clearSpy = vi.spyOn(globalThis, "clearInterval");
    (component as any).timerHandle = setInterval(() => {}, 1000);

    component.ngOnDestroy();

    expect(clearSpy).toHaveBeenCalled();
    expect((component as any).timerHandle).toBeNull();
  });

  it("ignores a second submit while the first is still pending", async () => {
    // A pending requestImport models the opener still showing its overwrite confirmation.
    let resolveRequest!: (proceed: boolean) => void;
    requestImport.mockReturnValue(new Promise<boolean>(resolve => (resolveRequest = resolve)));
    await createWith(of([{ name: "gpt-4" }]));
    component.importForm.setValue({ file: { name: "x.ipynb" } as NzUploadFile, model: "gpt-4" });

    const first = component.onSubmit();
    const second = component.onSubmit(); // double-click while the first is in flight

    expect(requestImport).toHaveBeenCalledTimes(1);
    expect(component.isSubmitting).toBe(true);

    resolveRequest(true);
    await Promise.all([first, second]);
  });

  it("re-enables submit after the opener declines, allowing another attempt", async () => {
    requestImport.mockResolvedValue(false);
    await createWith(of([{ name: "gpt-4" }]));
    component.importForm.setValue({ file: { name: "x.ipynb" } as NzUploadFile, model: "gpt-4" });

    await component.onSubmit();
    expect(component.isSubmitting).toBe(false);

    // The guard has cleared, so a second attempt is allowed and calls the opener again.
    await component.onSubmit();
    expect(requestImport).toHaveBeenCalledTimes(2);
  });

  it("onSubmit does nothing while the form is invalid", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    component.importForm.setValue({ file: null, model: "" });

    await component.onSubmit();

    expect(requestImport).not.toHaveBeenCalled();
    expect(modalRef.close).not.toHaveBeenCalled();
  });

  it("onCancel closes the modal without importing", async () => {
    await createWith(of([{ name: "gpt-4" }]));

    component.onCancel();

    expect(modalRef.close).toHaveBeenCalledWith();
    expect(requestImport).not.toHaveBeenCalled();
  });
});
