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

import { DROP_REJECTION_LATCH_MS, NotebookImportModalComponent } from "./notebook-import-modal.component";
import { NotebookMigrationService } from "../../service/notebook-migration/notebook-migration.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { GuiConfigService } from "../../../common/service/gui-config.service";

describe("NotebookImportModalComponent", () => {
  let fixture: ComponentFixture<NotebookImportModalComponent>;
  let component: NotebookImportModalComponent;
  let notebookMigrationService: NotebookMigrationService;
  let modalRef: { close: ReturnType<typeof vi.fn>; updateConfig: ReturnType<typeof vi.fn> };
  // The opener-supplied gate; tests set its resolved value to drive close vs stay-open.
  let requestImport: ReturnType<typeof vi.fn>;

  // Configures the modal with the given models$ stream, then creates and renders it.
  async function createWith(models$: unknown, rNotebookMigrationEnabled = false): Promise<void> {
    await TestBed.configureTestingModule({
      imports: [NotebookImportModalComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [
        { provide: NzModalRef, useValue: modalRef },
        { provide: NZ_MODAL_DATA, useValue: { requestImport } },
        ...commonTestProviders,
      ],
    }).compileComponents();
    TestBed.inject(GuiConfigService).env.rNotebookMigrationEnabled = rNotebookMigrationEnabled;

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

    const result = component.beforeUpload(file, [file]);

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

    expect(requestImport).toHaveBeenCalledWith(file, "gpt-4", "python");
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

  it("wires document visibilitychange to the host listener that repaints the stopwatch", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    // Spying after creation still observes the call: the generated host handler resolves
    // ctx.onVisibilityChange at dispatch time. The method body is empty on purpose, so the
    // wiring is the only thing worth asserting.
    const handler = vi.spyOn(component, "onVisibilityChange");

    document.dispatchEvent(new Event("visibilitychange"));

    expect(handler).toHaveBeenCalled();
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

  it("swaps the diagram, description and upload target when the Python tab is selected", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const root = fixture.nativeElement as HTMLElement;
    // Once rendered, a tab pane stays in the DOM and is only hidden, so assert on the
    // active pane rather than the whole modal.
    const pane = () => root.querySelector(".ant-tabs-tabpane-active") as HTMLElement;
    const accept = () => pane().querySelector("input[type='file']")?.getAttribute("accept");

    // The src is asserted, not just the alt: a wrong path would otherwise pass here and 404 in
    // the browser. ngSrc rewrites the attribute, so match on a suffix rather than the whole value.
    const diagramSrc = () => pane().querySelector("img")?.getAttribute("src") ?? "";

    expect(pane().querySelector("img[alt='Notebook to Workflow']")).not.toBeNull();
    expect(diagramSrc()).toContain("assets/notebook_migration_tool/tool_popup_diagram.png");
    expect(accept()).toBe(".ipynb");

    component.onTabChange(1);
    fixture.detectChanges();

    expect(pane().querySelector("img[alt='Notebook to Workflow']")).toBeNull();
    expect(pane().querySelector("img[alt='Python File to Workflow']")).not.toBeNull();
    expect(diagramSrc()).toContain("assets/notebook_migration_tool/python_file_diagram.png");
    expect(accept()).toBe(".py");
    expect(pane().textContent).toContain("Upload Python File");
  });

  it("drops the selected file when switching tabs so it cannot be staged under the wrong extension", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    component.importForm.setValue({ file: { name: "demo.ipynb" }, model: "gpt-4" });

    component.onTabChange(1);
    fixture.detectChanges();

    expect(component.importForm.get("file")?.value).toBeNull();
    // The model applies to either input, so it survives the switch.
    expect(component.importForm.get("model")?.value).toBe("gpt-4");
    expect((fixture.nativeElement as HTMLElement).textContent).not.toContain("Selected file:");
  });

  it("enables Submit on the Python tab once a file and model are chosen", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const root = fixture.nativeElement as HTMLElement;
    const submit = () => root.querySelector(".import-modal-footer button[nzType='primary']") as HTMLButtonElement;

    component.onTabChange(1);
    fixture.detectChanges();
    // Switching tabs clears the file, so the form is incomplete until one is staged.
    expect(submit().disabled).toBe(true);

    component.importForm.setValue({ file: { name: "demo.py" }, model: "gpt-4" });
    fixture.detectChanges();

    expect(submit().disabled).toBe(false);
  });

  it("submits a Python file to the opener the same way it submits a notebook", async () => {
    requestImport.mockResolvedValue(true);
    await createWith(of([{ name: "gpt-4" }]));
    const file = { name: "demo.py" } as NzUploadFile;
    component.onTabChange(1);
    component.importForm.setValue({ file, model: "gpt-4" });

    await component.onSubmit();

    expect(requestImport).toHaveBeenCalledWith(file, "gpt-4", "python");
    expect(modalRef.close).toHaveBeenCalledWith();
  });

  it("the footer Cancel button closes the modal", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const cancel = (fixture.nativeElement as HTMLElement).querySelector(
      ".import-modal-footer button:not([nzType='primary'])"
    ) as HTMLButtonElement;

    // Clicked rather than calling onCancel(), so the template's click binding is exercised too.
    cancel.click();

    expect(modalRef.close).toHaveBeenCalledWith();
    expect(requestImport).not.toHaveBeenCalled();
  });

  it("the footer Submit button runs the import with the staged file and model", async () => {
    requestImport.mockResolvedValue(true);
    await createWith(of([{ name: "gpt-4" }]));
    const file = { name: "demo.ipynb" } as NzUploadFile;
    component.importForm.setValue({ file, model: "gpt-4" });
    fixture.detectChanges();

    // Clicked rather than calling onSubmit(), so the template's click binding is exercised too.
    (
      (fixture.nativeElement as HTMLElement).querySelector(
        ".import-modal-footer button[nzType='primary']"
      ) as HTMLButtonElement
    ).click();
    await fixture.whenStable();

    expect(requestImport).toHaveBeenCalledWith(file, "gpt-4", "python");
    expect(modalRef.close).toHaveBeenCalledWith();
  });

  it("selects the Python tab from a click on its header, not just a direct call", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const headers = (fixture.nativeElement as HTMLElement).querySelectorAll(".ant-tabs-tab");
    expect(headers.length).toBe(3);

    (headers[1] as HTMLElement).click();
    // nz-tabs emits nzSelectedIndexChange from a microtask inside ngAfterContentChecked,
    // so the handler has not run until the queue drains.
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();

    expect(component.selectedTabIndex).toBe(1);
    expect(
      (fixture.nativeElement as HTMLElement)
        .querySelector(".ant-tabs-tabpane-active input[type='file']")
        ?.getAttribute("accept")
    ).toBe(".py");
  });

  it("restores the notebook upload target when switching back", async () => {
    await createWith(of([{ name: "gpt-4" }]));
    const root = fixture.nativeElement as HTMLElement;
    const pane = () => root.querySelector(".ant-tabs-tabpane-active") as HTMLElement;
    const submit = () => root.querySelector(".import-modal-footer button[nzType='primary']") as HTMLButtonElement;

    component.onTabChange(1);
    fixture.detectChanges();
    expect(pane().querySelector("input[type='file']")?.getAttribute("accept")).toBe(".py");

    component.onTabChange(0);
    component.importForm.setValue({ file: { name: "demo.ipynb" }, model: "gpt-4" });
    fixture.detectChanges();

    expect(pane().querySelector("input[type='file']")?.getAttribute("accept")).toBe(".ipynb");
    expect(submit().disabled).toBe(false);
  });
  describe("Python folder tab", () => {
    // What beforeUpload actually receives: ng-zorro's attachUid tacks a uid onto the browser File
    // and hands that over. originFileObj is only set later, when it builds the display list, so a
    // fixture that wraps the File does not exercise the path the real picker takes.
    function pickedFile(relativePath: string): NzUploadFile {
      const file = new File([""], relativePath.split("/").pop() ?? relativePath);
      Object.defineProperty(file, "webkitRelativePath", { value: relativePath });
      (file as unknown as NzUploadFile).uid = relativePath;
      return file as unknown as NzUploadFile;
    }

    // The wrapped shape, which the same code has to keep reading.
    function wrappedFile(relativePath: string): NzUploadFile {
      const file = pickedFile(relativePath);
      return { uid: relativePath, name: file.name, originFileObj: file } as unknown as NzUploadFile;
    }

    it("renders its own diagram and a directory-picking input", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      const root = fixture.nativeElement as HTMLElement;

      component.onTabChange(2);
      fixture.detectChanges();

      const pane = root.querySelector(".ant-tabs-tabpane-active") as HTMLElement;
      expect(pane.querySelector("img[alt='Python Folder to Workflow']")).not.toBeNull();
      // webkitdirectory is what makes the picker select a folder rather than a file.
      expect(pane.querySelector("input[type='file']")?.hasAttribute("webkitdirectory")).toBe(true);
      // No accept filter, so a folder with no .py still reaches the service, which explains why.
      expect(pane.querySelector("input[type='file']")?.getAttribute("accept")).toBeFalsy();
    });

    it("stores the whole picked list, not the single file it was called with", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(2);
      const files = [pickedFile("proj/main.py"), pickedFile("proj/pkg/train.py")];

      const result = component.beforeUpload(files[0], files);

      expect(result).toBe(false);
      expect(component.importForm.get("file")?.value).toBe(files);
    });

    it("patches the form once for a whole directory pick", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(2);
      const files = [pickedFile("proj/a.py"), pickedFile("proj/b.py"), pickedFile("proj/c.py")];
      const patch = vi.spyOn(component.importForm, "patchValue");

      // The picker calls beforeUpload once per file, handing the same list every time.
      files.forEach(file => component.beforeUpload(file, files));

      expect(patch).toHaveBeenCalledTimes(1);
    });

    it("names the selected folder rather than listing its files", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(2);
      component.importForm.patchValue({ file: [pickedFile("proj/main.py"), pickedFile("proj/pkg/train.py")] });
      fixture.detectChanges();

      expect((fixture.nativeElement as HTMLElement).textContent).toContain("Selected folder: proj");
    });

    it("names the folder just the same when the file arrives wrapped", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(2);
      component.importForm.patchValue({ file: [wrappedFile("proj/main.py")] });
      fixture.detectChanges();

      expect((fixture.nativeElement as HTMLElement).textContent).toContain("Selected folder: proj");
    });

    it("hands the whole list to requestImport on submit", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(2);
      const files = [pickedFile("proj/main.py")];
      component.importForm.setValue({ file: files, model: "gpt-4" });

      await component.onSubmit();

      expect(requestImport).toHaveBeenCalledWith(files, "gpt-4", "python");
      expect(modalRef.close).toHaveBeenCalledWith();
    });

    // A dropped directory reaches beforeUpload one file at a time, each in its own one-element
    // list, and ng-zorro attaches no path to any of them. Modelled exactly, because the picker
    // fixtures above would never catch this.
    function droppedFile(name: string): NzUploadFile {
      const file = new File([""], name);
      (file as unknown as NzUploadFile).uid = name;
      return file as unknown as NzUploadFile;
    }

    it("stages nothing when a folder is dropped instead of picked", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(2);
      const dropped = [droppedFile("main.py"), droppedFile("train.py")];

      dropped.forEach(file => expect(component.beforeUpload(file, [file])).toBe(false));
      fixture.detectChanges();

      // Accepting one would convert a single arbitrary file as if it were the whole project.
      expect(component.importForm.get("file")?.value).toBeNull();
      expect((fixture.nativeElement as HTMLElement).textContent).not.toContain("Selected folder");
    });

    it("tells the user once per drop, not once per dropped file", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(2);
      const error = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});
      const dropped = Array.from({ length: 5 }, (_unused, index) => droppedFile(`f${index}.py`));

      dropped.forEach(file => component.beforeUpload(file, [file]));

      expect(error).toHaveBeenCalledTimes(1);
      expect(error.mock.calls[0][0]).toContain("Drop is not supported");
    });

    it("keeps the message latched however long the directory walk takes", async () => {
      vi.useFakeTimers();
      try {
        await createWith(of([{ name: "gpt-4" }]));
        component.onTabChange(2);
        const error = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});

        // One slow walk: files keep arriving, each gap shorter than the latch but the whole
        // walk far longer, which is what a fixed throttle rate would get wrong.
        for (let index = 0; index < 5; index++) {
          component.beforeUpload(droppedFile(`f${index}.py`), [droppedFile(`f${index}.py`)]);
          vi.advanceTimersByTime(DROP_REJECTION_LATCH_MS - 500);
        }

        expect(error).toHaveBeenCalledTimes(1);

        // Once the burst goes quiet the latch releases, so a second drop is reported again.
        vi.advanceTimersByTime(DROP_REJECTION_LATCH_MS + 500);
        component.beforeUpload(droppedFile("again.py"), [droppedFile("again.py")]);

        expect(error).toHaveBeenCalledTimes(2);
      } finally {
        vi.useRealTimers();
      }
    });

    it("still accepts a dropped file on the single-file tabs", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(1);
      const dropped = droppedFile("script.py");

      expect(component.beforeUpload(dropped, [dropped])).toBe(false);
      expect(component.importForm.get("file")?.value).toBe(dropped);
    });

    it("names a single picked file without the plural", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(2);
      component.importForm.patchValue({ file: [{ uid: "1", name: "a.py" } as NzUploadFile] });
      fixture.detectChanges();

      expect((fixture.nativeElement as HTMLElement).textContent).toContain("Selected folder: 1 file");
      expect((fixture.nativeElement as HTMLElement).textContent).not.toContain("1 files");
    });

    it("shows nothing for an empty list, rather than naming a folder with no files", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(2);
      component.importForm.patchValue({ file: [] });
      fixture.detectChanges();

      expect(component.selectionSummary).toBeNull();
      expect((fixture.nativeElement as HTMLElement).textContent).not.toContain("Selected folder");
    });

    it("drops a folder selection when switching away", async () => {
      await createWith(of([{ name: "gpt-4" }]));
      component.onTabChange(2);
      component.importForm.patchValue({ file: [pickedFile("proj/main.py")] });

      component.onTabChange(1);
      fixture.detectChanges();

      expect(component.importForm.get("file")?.value).toBeNull();
      expect((fixture.nativeElement as HTMLElement).textContent).not.toContain("Selected folder");
    });
  });

  describe("R tab", () => {
    const root = () => fixture.nativeElement as HTMLElement;
    const pane = () => root().querySelector(".ant-tabs-tabpane-active") as HTMLElement;
    const accept = () => pane().querySelector("input[type='file']")?.getAttribute("accept");

    async function openRTab(): Promise<void> {
      await createWith(of([{ name: "gpt-4" }]), true);
      component.onTabChange(3);
      fixture.detectChanges();
    }

    it("is hidden while the R flag is off", async () => {
      await createWith(of([{ name: "gpt-4" }]));

      expect(root().querySelectorAll(".ant-tabs-tab").length).toBe(3);
    });

    it("shows its diagram and takes R notebooks by default", async () => {
      await openRTab();

      expect(root().querySelectorAll(".ant-tabs-tab").length).toBe(4);
      expect(pane().querySelector("img[alt='R to Workflow']")).not.toBeNull();
      expect(accept()).toBe(".ipynb");
      expect(root().querySelectorAll(".ant-tabs-tab")[3].textContent?.trim()).toBe("R Code");
      expect(pane().querySelector(".import-modal-upload-row nz-segmented")).not.toBeNull();
    });

    it("switches to R files from the switch and drops the staged notebook", async () => {
      await openRTab();
      component.importForm.patchValue({ file: { name: "analysis.ipynb" } });

      const fileOption = Array.from(pane().querySelectorAll("label")).find(
        label => label.textContent?.trim() === "File"
      ) as HTMLElement;
      fileOption.click();
      fixture.detectChanges();

      expect(component.rInputKind.value).toBe("file");
      expect(accept()).toBe(".R,.r");
      expect(component.importForm.get("file")?.value).toBeNull();
    });

    it("keeps the chosen input kind when the tab is re-rendered", async () => {
      await openRTab();
      component.rInputKind.setValue("file");
      component.onTabChange(0);
      fixture.detectChanges();

      component.onTabChange(3);
      fixture.detectChanges();

      expect(accept()).toBe(".R,.r");
    });

    it("submits with the R language", async () => {
      await openRTab();
      const file = { name: "analysis.R" } as NzUploadFile;
      component.importForm.setValue({ file, model: "gpt-4" });

      await component.onSubmit();

      expect(requestImport).toHaveBeenCalledWith(file, "gpt-4", "r");
    });
  });
});
