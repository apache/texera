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

import { FormlyFieldConfig } from "@ngx-formly/core";
import { isDefined } from "../util/predicate";

import { Observable } from "rxjs";
import { FORM_DEBOUNCE_TIME_MS } from "../../workspace/service/execute-workflow/execute-workflow.service";
import { debounceTime, distinctUntilChanged, filter, share } from "rxjs/operators";
import { HideType, ValueRuleSet } from "../../workspace/types/custom-json-schema.interface";
import { PortSchema } from "../../workspace/types/workflow-compiling.interface";
import { AbstractControl } from "@angular/forms";

export function getFieldByName(fieldName: string, fields: FormlyFieldConfig[]): FormlyFieldConfig | undefined {
  return fields.filter((field, _, __) => field.key === fieldName)[0];
}

export function setHideExpression(toggleHidden: string[], fields: FormlyFieldConfig[], hiddenBy: string): void {
  toggleHidden.forEach(hiddenFieldName => {
    const fieldToBeHidden = getFieldByName(hiddenFieldName, fields);
    if (isDefined(fieldToBeHidden)) {
      fieldToBeHidden.expressions = { hide: "!field.parent.model." + hiddenBy };
    }
  });
}

type ValueRule = ValueRuleSet["allOf"][number]["then"];

/**
 * The one branch of `valueRules` that the row's current contents select, or undefined where
 * none does. A branch names its sibling fields and the values of theirs it applies to, so the
 * row model is what decides; `field.parent.model` is that row for an array item and the
 * operator itself for a top-level field.
 */
export function matchingValueRule(rules: ValueRuleSet | undefined, rowModel: any): ValueRule | undefined {
  if (!isDefined(rules) || !isDefined(rowModel)) {
    return undefined;
  }
  return rules.allOf.find(branch =>
    Object.entries(branch.if).every(([sibling, condition]) => (condition.valEnum ?? []).includes(rowModel[sibling]))
  )?.then;
}

/**
 * Validator holding a field to whichever branch of `valueRules` currently applies.
 *
 * An empty value passes: whether emptiness is allowed is `required`'s business, and a field
 * that answers twice would report the wrong thing once. The numeric branches accept what
 * JavaScript reads as a number, which is slightly narrower than the Python converters on the
 * other end (they take `1_000` and `inf`); erring narrow here would be wrong for a field whose
 * accepted set is open, but these two are bounded and the values it turns away are ones no one
 * types into a hyperparameter.
 */
export function createValueRulesValidator(rules: ValueRuleSet) {
  return (control: AbstractControl, field: FormlyFieldConfig): boolean => {
    const rule = matchingValueRule(rules, field?.parent?.model);
    if (!isDefined(rule)) {
      return true;
    }
    const value = control.value;
    if (value === null || value === undefined || value === "") {
      return true;
    }
    const text = String(value).trim();
    if (isDefined(rule.enum)) {
      return rule.enum.includes(text);
    }
    if (isDefined(rule.pattern)) {
      // anchored the way the declaration writes it, so the same expression judges the value
      // here, in the operator's own tests and in the generated Python
      return new RegExp(rule.pattern).test(String(value));
    }
    if (rule.type === "integer") {
      return /^[-+]?\d+$/.test(text);
    }
    if (rule.type === "number") {
      return text.length > 0 && Number.isFinite(Number(text));
    }
    return true;
  };
}

/** Says what the field will take, naming the branch rather than the rule that rejected it. */
export function valueRulesValidationMessage(_err: unknown, field: FormlyFieldConfig): string {
  const rule = matchingValueRule(field?.props?.valueRules, field?.parent?.model);
  if (isDefined(rule?.enum)) {
    return `must be one of ${rule.enum.join(", ")}`;
  }
  if (isDefined(rule?.pattern)) {
    // a pattern covers shapes no short phrase names, so point at a value that works instead
    const example = rule.examples?.[0];
    return isDefined(example)
      ? `is not a value this parameter takes, such as ${example}`
      : "is not a value this parameter takes";
  }
  if (rule?.type === "integer") {
    return "must be a whole number";
  }
  return "must be a number";
}

/* Factory function to make functions that hide expressions for a particular field */
export function createShouldHideFieldFunc(
  hideTarget: string,
  hideType: HideType,
  hideExpectedValue: string,
  hideOnNull: boolean | undefined
) {
  let shared_regex: RegExp | null = null;

  const hideFunc = (field?: FormlyFieldConfig | undefined) => {
    const model = field?.parent?.model;
    if (model === null || model === undefined) {
      console.debug("Formly main model not detected. Hiding will fail.");
      return false;
    }

    let targetFieldValue: any = model[hideTarget];
    if (targetFieldValue === null || targetFieldValue === undefined) {
      // console.debug("Formly model does not contain hide target. Formly does not know what to hide.");
      return hideOnNull === true;
    }

    switch (hideType) {
      case "regex":
        if (shared_regex === null) shared_regex = new RegExp(`^(${hideExpectedValue})$`);
        return shared_regex.test(targetFieldValue);
      case "equals":
        return targetFieldValue.toString() === hideExpectedValue;
    }
  };

  return hideFunc;
}

export function setChildTypeDependency(
  attributes: Readonly<Record<string, PortSchema | undefined>> | undefined,
  parentName: string,
  fields: FormlyFieldConfig[],
  childName: string
): void {
  const timestampFieldNames = Object.values(attributes || {})
    .flat()
    .filter(attribute => {
      return attribute?.attributeType === "timestamp";
    })
    .map(attribute => attribute?.attributeName);

  if (timestampFieldNames) {
    const childField = getFieldByName(childName, fields);
    if (isDefined(childField)) {
      childField.expressions = {
        // 'type': 'string',
        // 'templateOptions.type': JSON.stringify(timestampFieldNames) + '.includes(model.' + parentName + ')? \'string\' : \'number\'',

        "templateOptions.description":
          JSON.stringify(timestampFieldNames) +
          ".includes(model." +
          parentName +
          ")? 'Input a datetime string' : 'Input a positive number'",
      };
    }
  }
}

/**
 * Handles the form change event stream observable,
 *  which corresponds to every event the json schema form library emits.
 *
 * Applies rules that transform the event stream to trigger reasonably and less frequently,
 *  such as debounce time and distinct condition.
 *
 * Then modifies the operator property to use the new form data.
 */
export function createOutputFormChangeEventStream(
  formChangeEvent: Observable<Record<string, unknown>>,
  modelCheck: (formData: Record<string, unknown>) => boolean
): Observable<Record<string, unknown>> {
  return (
    formChangeEvent
      // set a debounce time to avoid events triggering too often
      //  and to circumvent a bug of the library - each action triggers event twice
      .pipe(
        debounceTime(FORM_DEBOUNCE_TIME_MS),
        // .do(evt => console.log(evt))
        // don't emit the event until the data is changed
        distinctUntilChanged(),
        // .do(evt => console.log(evt))
        // don't emit the event if form data is same with current actual data
        // also check for other unlikely circumstances (see below)
        filter(formData => modelCheck(formData)),
        // share() because the original observable is a hot observable
        share()
      )
  );
}
