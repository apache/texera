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
import { FieldWrapper, FormlyFieldConfig } from "@ngx-formly/core";
import { merge } from "lodash-es";

/**
 * A tick box beside an operator property, in the property editor, for an author to choose
 * whether it appears on the Form View. Rendered only while choosing.
 */
@Component({
  selector: "texera-expose-property-wrapper",
  templateUrl: "./expose-property-wrapper.component.html",
  styleUrls: ["./expose-property-wrapper.component.scss"],
})
export class ExposePropertyWrapperComponent extends FieldWrapper {
  /** Add this wrapper to a field, carrying state + callback in `props`; `form-field` stays
   *  outermost so label/error rendering is untouched. */
  public static decorate(config: FormlyFieldConfig, exposed: boolean, toggle: (checked: boolean) => void): void {
    merge(config, {
      wrappers: [...(config.wrappers ?? ["form-field"]), "expose-property-wrapper"],
      props: { ...config.props, exposed, toggleExposed: toggle },
    });
  }

  public onToggle(event: Event): void {
    this.props["toggleExposed"]((event.target as HTMLInputElement).checked);
  }
}
