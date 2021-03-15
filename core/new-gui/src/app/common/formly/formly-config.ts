import { NullTypeComponent } from './null.type';
import { ArrayTypeComponent } from './array.type';
import { ObjectTypeComponent } from './object.type';
import { MultiSchemaTypeComponent } from './multischema.type';
import { FormlyFieldConfig } from '@ngx-formly/core';
import { CodeareaCustomTemplateComponent } from '../../workspace/component/codearea-custom-template/codearea-custom-template.component';

/**
 * Configuration for using Json Schema with Formly.
 * This config is copy-pasted from official documentation,
 * see https://formly.dev/examples/advanced/json-schema
 */
export const TEXERA_FORMLY_CONFIG = {
  validationMessages: [
    { name: 'required', message: 'This field is required' },
    { name: 'null', message: 'should be null' },
    { name: 'minlength', message: minlengthValidationMessage },
    { name: 'maxlength', message: maxlengthValidationMessage },
    { name: 'min', message: minValidationMessage },
    { name: 'max', message: maxValidationMessage },
    { name: 'multipleOf', message: multipleOfValidationMessage },
    { name: 'exclusiveMinimum', message: exclusiveMinimumValidationMessage },
    { name: 'exclusiveMaximum', message: exclusiveMaximumValidationMessage },
    { name: 'minItems', message: minItemsValidationMessage },
    { name: 'maxItems', message: maxItemsValidationMessage },
    { name: 'uniqueItems', message: 'should NOT have duplicate items' },
    { name: 'const', message: constValidationMessage },
  ],
  types: [
    { name: 'string', extends: 'input' },
    {
      name: 'number',
      extends: 'input',
      defaultOptions: {
        templateOptions: {
          type: 'number',
        },
      },
    },
    {
      name: 'integer',
      extends: 'input',
      defaultOptions: {
        templateOptions: {
          type: 'number',
        },
      },
    },
    { name: 'boolean', extends: 'checkbox' },
    { name: 'enum', extends: 'select' },
    { name: 'null', component: NullTypeComponent, wrappers: ['form-field'] },
    { name: 'array', component: ArrayTypeComponent },
    { name: 'object', component: ObjectTypeComponent },
    { name: 'multischema', component: MultiSchemaTypeComponent },
    { name: 'codearea', component: CodeareaCustomTemplateComponent},
  ],
};

export function minItemsValidationMessage(err: any, field: FormlyFieldConfig) {
  return `should NOT have fewer than ${field.templateOptions?.minItems} items`;
}

export function maxItemsValidationMessage(err: any, field: FormlyFieldConfig) {
  return `should NOT have more than ${field.templateOptions?.maxItems} items`;
}

export function minlengthValidationMessage(err: any, field: FormlyFieldConfig) {
  return `should NOT be shorter than ${field.templateOptions?.minLength} characters`;
}

export function maxlengthValidationMessage(err: any, field: FormlyFieldConfig) {
  return `should NOT be longer than ${field.templateOptions?.maxLength} characters`;
}

export function minValidationMessage(err: any, field: FormlyFieldConfig) {
  return `should be >= ${field.templateOptions?.min}`;
}

export function maxValidationMessage(err: any, field: FormlyFieldConfig) {
  return `should be <= ${field.templateOptions?.max}`;
}

export function multipleOfValidationMessage(err: any, field: FormlyFieldConfig) {
  return `should be multiple of ${field.templateOptions?.step}`;
}

export function exclusiveMinimumValidationMessage(err: any, field: FormlyFieldConfig) {
  return `should be > ${field.templateOptions?.exclusiveMinimum}`;
}

export function exclusiveMaximumValidationMessage(err: any, field: FormlyFieldConfig) {
  return `should be < ${field.templateOptions?.exclusiveMaximum}`;
}

export function constValidationMessage(err: any, field: FormlyFieldConfig) {
  return `should be equal to constant "${field.templateOptions?.const}"`;
}

