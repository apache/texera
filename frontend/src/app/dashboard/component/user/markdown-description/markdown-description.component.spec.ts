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

import { Provider, SimpleChange } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { MarkdownService } from "ngx-markdown";
import { MarkdownDescriptionComponent } from "./markdown-description.component";

describe("MarkdownDescriptionComponent", () => {
  // Echo non-empty markdown as trivial HTML so `renderedDescription` is observable
  // without pulling in the real (config-dependent) ngx-markdown parser.
  const parse = vi.fn((text: string) => (text ? `<p>${text}</p>` : ""));

  // Passing `modalData` provides NZ_MODAL_DATA (the "opened in a modal" case);
  // omitting it leaves the optional injection null (the inline case).
  async function createFixture(modalData?: {
    description?: string;
  }): Promise<ComponentFixture<MarkdownDescriptionComponent>> {
    const providers: Provider[] = [{ provide: MarkdownService, useValue: { parse } }];
    if (modalData !== undefined) {
      providers.push({ provide: NZ_MODAL_DATA, useValue: modalData });
    }
    await TestBed.configureTestingModule({
      imports: [MarkdownDescriptionComponent],
      providers,
    }).compileComponents();
    return TestBed.createComponent(MarkdownDescriptionComponent);
  }

  // A non-first change of the `description` input.
  const descriptionChange = (previous: string, current: string): { description: SimpleChange } => ({
    description: new SimpleChange(previous, current, false),
  });

  beforeEach(() => parse.mockClear());

  it("should create and render the preview template when not opened in a modal", async () => {
    const fixture = await createFixture();
    fixture.componentInstance.description = "hello";
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.componentInstance).toBeTruthy();
    expect(fixture.nativeElement.querySelector(".preview-box")).toBeTruthy();
  });

  it("shows the Edit action in preview mode when editable", async () => {
    const fixture = await createFixture();
    fixture.componentInstance.editable = true;
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector(".md-actions")).toBeTruthy();
  });

  it("hides the Edit action in preview mode when not editable", async () => {
    const fixture = await createFixture();
    fixture.componentInstance.editable = false;
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector(".md-actions")).toBeNull();
  });

  it("binds enableViewMore by toggling the preview collapsed class", async () => {
    const fixture = await createFixture();
    fixture.componentInstance.enableViewMore = true;
    fixture.detectChanges();

    const previewBox = fixture.nativeElement.querySelector(".preview-box") as HTMLElement;
    expect(previewBox.classList.contains("collapsed")).toBe(true);

    fixture.componentInstance.enableViewMore = false;
    fixture.detectChanges();
    expect(previewBox.classList.contains("collapsed")).toBe(false);
  });
  it("ngOnInit starts in preview mode and seeds editingContent from description (inline)", async () => {
    const fixture = await createFixture();
    const component = fixture.componentInstance;
    component.description = "content";
    fixture.detectChanges();

    expect(component.currentMode).toBe("preview");
    expect(component.editingContent).toBe("content");
    expect(component.editable).toBe(false);
  });

  it("ngOnInit enters edit mode and pulls the description from modal data", async () => {
    const fixture = await createFixture({ description: "from modal" });
    const component = fixture.componentInstance;
    fixture.detectChanges();

    expect(component.editable).toBe(true);
    expect(component.currentMode).toBe("edit");
    expect(component.description).toBe("from modal");
    expect(component.editingContent).toBe("from modal");
  });

  it("ngOnInit defaults the description to empty when modal data omits it", async () => {
    const fixture = await createFixture({});
    const component = fixture.componentInstance;
    fixture.detectChanges();

    expect(component.description).toBe("");
    expect(component.currentMode).toBe("edit");
  });

  it("ngOnChanges refreshes editingContent from a new description while in preview mode", async () => {
    const fixture = await createFixture();
    const component = fixture.componentInstance;
    component.description = "old";
    fixture.detectChanges();
    parse.mockClear();

    component.description = "new";
    component.ngOnChanges(descriptionChange("old", "new"));

    expect(component.editingContent).toBe("new");
    expect(parse).toHaveBeenCalledWith("new");
  });

  it("ngOnChanges leaves editingContent untouched while in edit mode (unsaved edits win)", async () => {
    const fixture = await createFixture({ description: "orig" });
    const component = fixture.componentInstance;
    fixture.detectChanges(); // modal -> edit mode
    component.editingContent = "user is typing";

    component.description = "external change";
    component.ngOnChanges(descriptionChange("orig", "external change"));

    expect(component.editingContent).toBe("user is typing");
  });

  it("ngOnChanges ignores the first change", async () => {
    const fixture = await createFixture();
    const component = fixture.componentInstance;
    component.description = "seed";
    fixture.detectChanges();
    component.editingContent = "seed";

    component.ngOnChanges({ description: new SimpleChange(undefined, "seed", true) });

    expect(component.editingContent).toBe("seed");
  });

  it("enterEditMode switches to edit mode only when editable", async () => {
    const fixture = await createFixture();
    const component = fixture.componentInstance;
    component.description = "d";
    fixture.detectChanges();

    component.editable = false;
    component.enterEditMode();
    expect(component.currentMode).toBe("preview");

    component.editable = true;
    component.enterEditMode();
    expect(component.currentMode).toBe("edit");
    expect(component.editingContent).toBe("d");
  });

  it("save emits descriptionChange and returns to preview mode when inline", async () => {
    const fixture = await createFixture();
    const component = fixture.componentInstance;
    component.description = "old";
    component.editable = true;
    fixture.detectChanges();
    component.enterEditMode();
    component.editingContent = "edited";

    const emitted: string[] = [];
    component.descriptionChange.subscribe(v => emitted.push(v));
    component.save();

    expect(emitted).toEqual(["edited"]);
    expect(component.description).toBe("edited");
    expect(component.currentMode).toBe("preview");
  });

  it("save stays in edit mode when opened in a modal", async () => {
    const fixture = await createFixture({ description: "x" });
    const component = fixture.componentInstance;
    fixture.detectChanges(); // modal -> edit mode
    component.editingContent = "y";

    const emitted: string[] = [];
    component.descriptionChange.subscribe(v => emitted.push(v));
    component.save();

    expect(emitted).toEqual(["y"]);
    expect(component.currentMode).toBe("edit");
  });

  it("cancel restores editingContent from description and returns to preview", async () => {
    const fixture = await createFixture();
    const component = fixture.componentInstance;
    component.description = "orig";
    component.editable = true;
    fixture.detectChanges();
    component.enterEditMode();
    component.editingContent = "unsaved";

    component.cancel();

    expect(component.editingContent).toBe("orig");
    expect(component.currentMode).toBe("preview");
  });

  it("renderMarkdown renders non-empty input and clears on blank input without parsing", async () => {
    const fixture = await createFixture();
    const component = fixture.componentInstance;
    fixture.detectChanges();

    component.renderMarkdown("# Hi");
    await fixture.whenStable();
    expect(parse).toHaveBeenCalledWith("# Hi");
    expect(component.renderedDescription).toBe("<p># Hi</p>");

    parse.mockClear();
    component.renderMarkdown("   ");
    expect(parse).not.toHaveBeenCalled();
    expect(component.renderedDescription).toBe("");
  });
});
