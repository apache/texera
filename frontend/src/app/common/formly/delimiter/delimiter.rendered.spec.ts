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

import { Component } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { FormGroup, ReactiveFormsModule } from "@angular/forms";
import { FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
import { FormlyJsonschema } from "@ngx-formly/core/json-schema";
import { FormlyNgZorroAntdModule } from "@ngx-formly/ng-zorro-antd";
import { TEXERA_FORMLY_CONFIG } from "../formly-config";
import { CUSTOM_DELIMITER } from "./delimiter-presets";
import { DelimiterTypeComponent } from "./delimiter.type";

// The property schemas the backend generates (UIWidget.UIWidgetCharDelimiter / UIWidgetRegexDelimiter),
// so this renders the widget the way the operator panel does: schema -> formly-jsonschema -> Texera config.
const CSV_SCHEMA = {
  type: "object",
  properties: {
    customDelimiter: {
      type: "string",
      title: "Delimiter",
      default: ",",
      maxLength: 1,
      widget: { formlyConfig: { type: "delimiter", props: { delimiterMode: "char" } } },
    },
  },
};

const UNNEST_SCHEMA = {
  type: "object",
  required: ["Delimiter"],
  properties: {
    Delimiter: {
      type: "string",
      default: ",",
      widget: { formlyConfig: { type: "delimiter", props: { delimiterMode: "regex" } } },
    },
  },
};

@Component({
  template:
    "<form [formGroup]='form'><formly-form [form]='form' [fields]='fields' [model]='model'></formly-form></form>",
  imports: [ReactiveFormsModule, FormlyModule],
})
class HostComponent {
  form = new FormGroup({});
  fields: FormlyFieldConfig[] = [];
  model: Record<string, unknown> = {};
}

describe("delimiter widget rendered from the operator schema", () => {
  let fixture: ComponentFixture<HostComponent>;
  let host: HostComponent;

  async function render(schema: object, model: Record<string, unknown>): Promise<void> {
    // No validation overrides: the panel only forces errors visible on a field with no validators, and
    // formly-jsonschema gives every field one, so whether an error shows is up to the widget itself.
    host.fields = [TestBed.inject(FormlyJsonschema).toFieldConfig(schema as never)];
    host.model = model;
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
  }

  const widget = (): DelimiterTypeComponent =>
    fixture.debugElement.query(el => el.componentInstance instanceof DelimiterTypeComponent).componentInstance;
  const customBox = (): HTMLInputElement | null => fixture.nativeElement.querySelector("input.delimiter-custom");
  const errorText = (): string => (fixture.nativeElement.querySelector("nz-form-control")?.textContent ?? "").trim();

  async function typeCustom(text: string): Promise<void> {
    widget().onSelect(CUSTOM_DELIMITER);
    fixture.detectChanges();
    const box = customBox()!;
    box.value = text;
    box.dispatchEvent(new Event("input"));
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
  }

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HostComponent, FormlyModule.forRoot(TEXERA_FORMLY_CONFIG), FormlyNgZorroAntdModule],
    }).compileComponents();
    fixture = TestBed.createComponent(HostComponent);
    host = fixture.componentInstance;
  });

  it("renders a CSV scan's delimiter as the picker, not a text box", async () => {
    await render(CSV_SCHEMA, { customDelimiter: "\t" });

    expect(widget().mode).toBe("char");
    expect(widget().selected).toBe("\t");
    expect(customBox()).toBeNull();
  });

  it("writes a picked preset into the operator's properties", async () => {
    await render(CSV_SCHEMA, { customDelimiter: "," });

    widget().onSelect("|");
    fixture.detectChanges();

    expect(host.form.value).toEqual({ customDelimiter: "|" });
  });

  it("flags a stored multi-character CSV delimiter with the schema's length message", async () => {
    await render(CSV_SCHEMA, { customDelimiter: ",;" });

    expect(host.form.valid).toBe(false);
    expect(errorText()).toContain("should NOT be longer than 1 characters");
  });

  it("renders Unnest String's delimiter as the regex picker", async () => {
    await render(UNNEST_SCHEMA, { Delimiter: "\\s+" });

    expect(widget().mode).toBe("regex");
    expect(widget().selected).toBe("\\s+");
  });

  it("shows why a custom regex does not compile, under the field", async () => {
    await render(UNNEST_SCHEMA, { Delimiter: "," });

    await typeCustom("[a-");

    expect(host.form.valid).toBe(false);
    expect(errorText()).toContain("Invalid regular expression");
  });

  it("warns under the field that a bare pipe would split every character", async () => {
    await render(UNNEST_SCHEMA, { Delimiter: "," });

    await typeCustom("|");

    expect(host.form.valid).toBe(false);
    expect(errorText()).toContain("matches an empty string");
  });

  it("warns under the field that an unescaped dot would leave nothing, and offers the escape", async () => {
    await render(UNNEST_SCHEMA, { Delimiter: "," });

    await typeCustom(".");

    expect(host.form.valid).toBe(false);
    expect(errorText()).toContain("only line breaks would be left");
    expect(errorText()).toContain("\\.");
  });

  it("does not show a false error for Java-only syntax the operator accepts", async () => {
    await render(UNNEST_SCHEMA, { Delimiter: "," });

    await typeCustom(",++");

    expect(host.form.valid).toBe(true);
  });

  it("lets \\t be typed despite the schema's one-character bound", async () => {
    await render(CSV_SCHEMA, { customDelimiter: "," });
    await typeCustom("#");

    expect(customBox()!.hasAttribute("maxlength")).toBe(false);
  });

  it("stores a typed \\t in a CSV scan as a real tab", async () => {
    await render(CSV_SCHEMA, { customDelimiter: "," });

    await typeCustom("\\t");

    expect(host.form.value).toEqual({ customDelimiter: "\t" });
    expect(host.form.valid).toBe(true);
  });

  it("clears the error once the pattern is fixed", async () => {
    await render(UNNEST_SCHEMA, { Delimiter: "," });
    await typeCustom("(");
    expect(host.form.valid).toBe(false);

    await typeCustom("\\s*,\\s*");

    expect(host.form.valid).toBe(true);
    expect(host.form.value).toEqual({ Delimiter: "\\s*,\\s*" });
    expect(errorText()).not.toContain("Invalid");
  });

  it("marks an emptied custom regex as required", async () => {
    await render(UNNEST_SCHEMA, { Delimiter: "," });

    await typeCustom("");

    expect(host.form.valid).toBe(false);
    expect(errorText()).toContain("This field is required");
  });
});
