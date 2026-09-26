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

import { FormControl } from "@angular/forms";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { FormlyFieldConfig } from "@ngx-formly/core";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { ValueRuleSet } from "../../types/custom-json-schema.interface";
import { ConstrainedValueComponent } from "./constrained-value.component";

describe("ConstrainedValueComponent", () => {
  // one branch of each shape a rule can take, keyed on the sibling `parameter`
  const rules: ValueRuleSet = {
    allOf: [
      {
        if: { parameter: { valEnum: ["kernel"] } },
        then: { enum: ["rbf", "linear", "poly", "sigmoid", "precomputed"] },
      },
      { if: { parameter: { valEnum: ["C"] } }, then: { type: "number", examples: ["1.0"] } },
      { if: { parameter: { valEnum: ["degree"] } }, then: { type: "integer", examples: ["3"] } },
      {
        if: { parameter: { valEnum: ["gamma"] } },
        then: { pattern: "^\\s*(?:scale|auto|[-+]?[0-9]*\\.?[0-9]+)\\s*$", examples: ["scale"] },
      },
    ],
  };

  let fixture: ComponentFixture<ConstrainedValueComponent>;
  let component: ConstrainedValueComponent;

  /** Puts the component in the row a real `paraList` item would give it. */
  const showFor = (parameter: string, value: string = ""): FormControl => {
    const formControl = new FormControl(value);
    (component as any).field = {
      key: "value",
      formControl,
      props: { valueRules: rules },
      parent: { model: { parameter } },
    } as FormlyFieldConfig;
    fixture.detectChanges();
    return formControl;
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ConstrainedValueComponent, NoopAnimationsModule],
    }).compileComponents();

    fixture = TestBed.createComponent(ConstrainedValueComponent);
    component = fixture.componentInstance;
  });

  it("offers the accepted values as a dropdown when the parameter is chosen from a set", () => {
    showFor("kernel");
    expect(component.acceptedValues).toEqual(["rbf", "linear", "poly", "sigmoid", "precomputed"]);
    expect(fixture.debugElement.query(By.css("nz-select"))).not.toBeNull();
    // nz-select carries a hidden input of its own, so look for ours rather than for any
    expect(fixture.debugElement.query(By.css("input[nz-input]"))).toBeNull();
  });

  it("gives a numeric parameter a number input instead", () => {
    showFor("C");
    expect(component.acceptedValues).toEqual([]);
    expect(component.inputType).toBe("number");
    expect(fixture.debugElement.query(By.css("nz-select"))).toBeNull();
    expect(fixture.debugElement.query(By.css("input[nz-input]")).nativeElement.type).toBe("number");
  });

  it("keeps a parameter described by a pattern on a text input, since it may hold a word", () => {
    showFor("gamma");
    expect(component.inputType).toBe("text");
    expect(fixture.debugElement.query(By.css("input[nz-input]")).nativeElement.type).toBe("text");
  });

  it("leaves a parameter no branch names as a plain text box", () => {
    showFor("metric_params");
    expect(component.acceptedValues).toEqual([]);
    expect(component.inputType).toBe("text");
  });

  it("follows the row when the parameter beside it changes", () => {
    showFor("kernel");
    expect(fixture.debugElement.query(By.css("nz-select"))).not.toBeNull();

    (component as any).field.parent.model.parameter = "C";
    fixture.detectChanges();

    expect(fixture.debugElement.query(By.css("nz-select"))).toBeNull();
    expect(fixture.debugElement.query(By.css("input[nz-input]")).nativeElement.type).toBe("number");
  });

  it("writes the control as a string whichever control produced the value", () => {
    const control = showFor("C");
    // a number input yields a number once its text parses
    component.write(0.1);
    expect(control.value).toBe("0.1");
    expect(typeof control.value).toBe("string");
  });

  it("writes an empty string when a dropdown is cleared", () => {
    const control = showFor("kernel", "rbf");
    component.write(null);
    expect(control.value).toBe("");
  });

  it("marks the control touched so the error shows on the first bad value", () => {
    const control = showFor("C");
    expect(control.touched).toBe(false);
    component.write("abc");
    expect(control.dirty).toBe(true);
    expect(control.touched).toBe(true);
  });

  it("reads an unset control as an empty string rather than null", () => {
    const control = showFor("C");
    control.setValue(null);
    expect(component.current).toBe("");
  });
});
