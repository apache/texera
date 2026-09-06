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
import { FormControl, ReactiveFormsModule } from "@angular/forms";
import { FormlyModule } from "@ngx-formly/core";
import { By } from "@angular/platform-browser";
import { NzSelectComponent, NzSelectModule } from "ng-zorro-antd/select";
import { SearchableSelectTypeComponent } from "./searchable-select.type";

describe("SearchableSelectTypeComponent", () => {
  let fixture: ComponentFixture<SearchableSelectTypeComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SearchableSelectTypeComponent, ReactiveFormsModule, FormlyModule.forRoot(), NzSelectModule],
    }).compileComponents();
  });

  function render(options: unknown[], multiple = false): NzSelectComponent {
    fixture = TestBed.createComponent(SearchableSelectTypeComponent);
    fixture.componentInstance.field = {
      key: "attribute",
      formControl: new FormControl(),
      props: { options, multiple },
      options: { showError: () => false },
    };
    fixture.detectChanges();
    return fixture.debugElement.query(By.directive(NzSelectComponent)).componentInstance as NzSelectComponent;
  }

  it("searches option labels case-insensitively", () => {
    const select = render(["customer_id", "order_total"]);

    expect(select.nzShowSearch).toBe(true);
    expect(select.nzFilterOption("TOTAL", { nzLabel: "order_total" } as any)).toBe(true);
    expect(select.nzFilterOption("missing", { nzLabel: "order_total" } as any)).toBe(false);
  });

  it("preserves default and multiple selection modes", () => {
    expect(render(["customer_id"]).nzMode).toBe("default");
    fixture.destroy();
    expect(render(["customer_id"], true).nzMode).toBe("multiple");
  });

  it("supports an empty option list", () => {
    expect(render([]).nzShowSearch).toBe(true);
  });
});
