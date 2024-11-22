import { FormlyFieldConfig } from "@ngx-formly/core";
import { isDefined } from "../util/predicate";

import { Observable } from "rxjs";
import { FORM_DEBOUNCE_TIME_MS } from "../../workspace/service/execute-workflow/execute-workflow.service";
import { debounceTime, distinctUntilChanged, filter, share } from "rxjs/operators";
import { HideType } from "../../workspace/types/custom-json-schema.interface";
import { PortInputSchema } from "../../workspace/types/workflow-compiling.interface";

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
  attributes: ReadonlyArray<PortInputSchema | undefined> | undefined,
  parentName: string,
  fields: FormlyFieldConfig[],
  childName: string
): void {
  const timestampFieldNames = attributes
    ?.flat()
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
