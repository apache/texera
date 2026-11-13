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
import { NgFor, NgIf } from "@angular/common";
import { FormControl, type AbstractControl } from "@angular/forms";
import { FieldArrayType, FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import {
  UiUdfParametersEditError,
  UiUdfParametersParseError,
} from "../../service/code-editor/ui-udf-parameters-parser.service";
import { UiUdfParametersSyncService } from "../../service/code-editor/ui-udf-parameters-sync.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { DATASET_INPUT_TYPE, MODEL_INPUT_TYPE } from "../../service/code-editor/ui-udf-parameters-parser.service";
import { UI_PARAMETER_TYPES } from "../../service/code-editor/ui-udf-parameter";
import type { AttributeType } from "../../types/workflow-compiling.interface";
import { fieldOperatorID, renderedInFormView, renderedReadOnly } from "../../util/custom-formly-type";
import { subFieldLabel, subFieldsInView } from "../../../common/formly/sub-field-view";
import { EditableLabelWrapperComponent } from "../../../common/formly/editable-label-wrapper/editable-label-wrapper.component";

/**
 * A column of the table. `path` is the column's sub-field path in the row, which is also the path
 * the Form View's author names and hides it under -- the two are the same thing, so the column can
 * be looked up in what the form published (see sub-field-view).
 */
type UiUdfParameterColumn = Readonly<{
  label: string;
  key: string;
  parentKey?: string;
  path: string;
  disabled: boolean;
}>;

const VALUE_COLUMN: UiUdfParameterColumn = { label: "Value", key: "value", path: "value", disabled: false };
const RESOURCE_VALUE_EDITOR = "resourcevalue";
const RESOURCE_INPUT_TYPES: ReadonlySet<string> = new Set([MODEL_INPUT_TYPE, DATASET_INPUT_TYPE]);

/** The resource a row's value names, or "" for free text. */
const resourceOf = (inputType?: string): string => (inputType && RESOURCE_INPUT_TYPES.has(inputType) ? inputType : "");

/** Renders inferred UDF UI parameters with editable values and locked name/type columns. */
@Component({
  selector: "texera-ui-udf-parameters",
  templateUrl: "./ui-udf-parameters.component.html",
  styleUrls: ["./ui-udf-parameters.component.scss"],
  imports: [
    NgIf,
    NgFor,
    FormlyModule,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
  ],
})
export class UiUdfParametersComponent extends FieldArrayType<FormlyFieldConfig> {
  private readonly disabledStateConfigured = new WeakMap<FormlyFieldConfig, boolean>();
  // The resource each row's value editor was configured for.
  private readonly rowResources = new WeakMap<FormlyFieldConfig, string>();
  // The controls of the cells currently locked, and the controls that carry the hold hook (see keepLocked).
  private readonly lockedControls = new WeakSet<AbstractControl>();
  private readonly hookedControls = new WeakSet<AbstractControl>();

  readonly fieldColumns: UiUdfParameterColumn[] = [
    VALUE_COLUMN,
    { label: "Name", key: "attributeName", parentKey: "attribute", path: "attribute.attributeName", disabled: true },
    { label: "Type", key: "attributeType", parentKey: "attribute", path: "attribute.attributeType", disabled: true },
  ];

  /**
   * The columns in view: all three on the operator property panel, and on a Form View card the ones
   * the author left (subFieldsInView, which keeps the array's identity so the header's ngFor does
   * not redraw). All three come back while the draft row is open: that row names and types the
   * parameter being added, so it needs them whatever the author hid, and a filtered header over its
   * three cells put the name box under the wrong heading (Copilot on #8846).
   *
   * Read live rather than fixed at populate: the form publishes its decisions on the field, which
   * this widget is handed after its first check.
   */
  get columns(): readonly UiUdfParameterColumn[] {
    return this.draftVisible ? this.fieldColumns : subFieldsInView(this.field ?? {}, this.fieldColumns);
  }

  readonly addParameterTypeOptions = UI_PARAMETER_TYPES;
  draftVisible = false;

  constructor(
    private workflowActionService: WorkflowActionService,
    private uiUdfParametersSyncService: UiUdfParametersSyncService,
    private notificationService: NotificationService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    super();
  }

  /** Whether this is a Form View card rather than the operator property panel; see editable. */
  get onFormView(): boolean {
    return renderedInFormView(this.field ?? {});
  }

  /**
   * Whether the table takes a new parameter here. On the operator property panel that is the
   * canvas's rule, as it always was: the workflow may be modified (write access, no run in flight).
   *
   * A Form View card asks those two halves separately, because the canvas's flag does not answer
   * either one there. Write access: the card is not a reader's, which the form marks on the field
   * itself (props.disabled, its one lock on a card; see renderedReadOnly). A writer's card is
   * editable in and out of edit mode, as every value on it is (adding a parameter is adding a row),
   * and the flag is not that -- the Form View holds it off outside edit mode, against structural
   * edits through its page, which hid the button on a card whose values could still be edited. No
   * run in flight: asked of the execute service, which owns that rule, since the flag that would
   * otherwise carry it is held off for the reason above. Adding a parameter inserts a declaration
   * into the UDF's own code, straight into the shared document, and code is what a run must not have
   * rewritten under it -- the property panel and the canvas code editor both refuse it then.
   *
   * The mark decides rather than the table's own control, which does not answer the question:
   * formly disables the leaf controls under a disabled field, not the array itself, so a reader's
   * table with no parameters yet reads as enabled (a reader's table with rows reads as disabled only
   * because every row's controls are).
   */
  get editable(): boolean {
    if (!this.onFormView) {
      return this.workflowActionService.checkWorkflowModificationEnabled();
    }
    return !renderedReadOnly(this.field) && !this.executeWorkflowService.isExecutionInFlight();
  }

  /**
   * Inserts the declaration into the operator's Python code; the row then appears through the normal
   * code sync. The operator is the one the field is bound to on a Form View card (nothing is
   * highlighted there, or another step is); on the operator property panel it is the highlighted one,
   * as it always was.
   */
  addParameter(nameInput: HTMLInputElement, attributeType: string): void {
    // The button is hidden then too; the check belongs with the edit, not only with the button.
    if (!this.editable) {
      return;
    }
    const operatorId =
      fieldOperatorID(this.field ?? {}) ??
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
    try {
      this.uiUdfParametersSyncService.addParameter(operatorId, nameInput.value, attributeType as AttributeType);
      this.draftVisible = false;
    } catch (error) {
      if (!(error instanceof UiUdfParametersEditError) && !(error instanceof UiUdfParametersParseError)) throw error;
      this.notificationService.error(`Could not add UDF parameter: ${error.message}`);
    }
  }

  override onPopulate(field: FormlyFieldConfig): void {
    this.configureRowTemplate(this.getFieldArrayTemplate(field));
    this.dropRowsWhoseResourceChanged(field, this.parameterRows(field));
    super.onPopulate(field);
    const rows = this.parameterRows(field);
    // A reader's card arrives with props.disabled on the field (the Form View's lock); the value
    // column's "editable" below must not undo it.
    const locked = field.props?.disabled === true;
    field.fieldGroup?.forEach((rowField, index) => this.configureRowFields(rowField, rows[index]?.inputType, locked));
    this.hostColumnLabels(field);
  }

  /**
   * While authoring on the Form View, a column's name box and eye live in this table's header, not
   * in its cells. The form's label wrapper decorates every row's sub-fields (the first row with the
   * controls, later rows with a static name); drawn in the cells, under a header that already shows
   * the name, that was the same name three times over. So whenever the header hosts a column's
   * controls (columnControls), every cell of that column tells its wrapper the label is drawn here.
   */
  private hostColumnLabels(field: FormlyFieldConfig): void {
    for (const column of this.fieldColumns) {
      if (!this.firstRowControls(field, column)) continue;
      field.fieldGroup?.forEach(rowField => {
        const cell = this.getColumnField(rowField, column);
        if (!cell) return;
        cell.props = {
          ...cell.props,
          [EditableLabelWrapperComponent.LABEL_HOSTED_PROP]: true,
          // The control's accessible name falls back to this; the heading sighted readers see is the
          // column's own label, so the two say the same thing ("Type", not "Attribute Type").
          schemaLabel: column.label,
        };
      });
    }
  }

  /**
   * The props the header's controls for a column are bound to: the first row's sub-field, the row
   * the form gave the controls to -- only on the Form View, and only while it did (`authoring`), so
   * the panel's header and a reader's stay plain text.
   */
  columnControls(column: UiUdfParameterColumn): FormlyFieldConfig["props"] | undefined {
    return this.firstRowControls(this.field ?? {}, column);
  }

  private firstRowControls(
    field: FormlyFieldConfig,
    column: UiUdfParameterColumn
  ): FormlyFieldConfig["props"] | undefined {
    const first = field.fieldGroup?.[0];
    if (!first || !renderedInFormView(field)) return undefined;
    const props = this.getColumnField(first, column)?.props;
    return props?.["authoring"] === true ? props : undefined;
  }

  /** The header's name box: the wrapper's own write-through, on the first row's props. */
  renameColumn(column: UiUdfParameterColumn, event: Event): void {
    const props = this.columnControls(column);
    if (props) EditableLabelWrapperComponent.rename(props, (event.target as HTMLInputElement).value);
  }

  /** The header's eye: the wrapper's own write-through, on the first row's props. */
  toggleColumnHidden(column: UiUdfParameterColumn): void {
    const props = this.columnControls(column);
    if (props) EditableLabelWrapperComponent.setHidden(props, !props["authorHidden"]);
  }

  /** Whether the author hid this column: its cells fade through their wrappers, the header with them. */
  columnHidden(column: UiUdfParameterColumn): boolean {
    return this.columnControls(column)?.["authorHidden"] === true;
  }

  /**
   * A column's header: the author's name for it on a Form View card, the fixed label otherwise --
   * not the schema's own title, which the operator property panel does not show either.
   */
  columnLabel(column: UiUdfParameterColumn): string {
    return subFieldLabel(this.field ?? {}, column.path, column.label);
  }

  /**
   * The parameter rows. Before Formly narrows `model` to this field's own array it still holds
   * the whole operator's properties, so the array is taken from this field's key in that case.
   */
  private parameterRows(field: FormlyFieldConfig): ReadonlyArray<{ inputType?: string } | undefined> {
    const model: unknown = Array.isArray(field.model)
      ? field.model
      : (field.model as Record<string, unknown> | undefined)?.[String(field.key)];
    return Array.isArray(model) ? model : [];
  }

  /** Finds the Formly field config that backs one visible column in a parameter row. */
  getColumnField(rowField: FormlyFieldConfig, column: UiUdfParameterColumn): FormlyFieldConfig | undefined {
    return this.getChildField(column.parentKey ? this.getChildField(rowField, column.parentKey) : rowField, column.key);
  }

  private getFieldArrayTemplate(field: FormlyFieldConfig): FormlyFieldConfig | undefined {
    return typeof field.fieldArray === "function" ? undefined : field.fieldArray;
  }

  private configureRowTemplate(rowField: FormlyFieldConfig | undefined): void {
    this.configureRowColumns(rowField, this.setDisabledMetadata.bind(this));
  }

  private configureRowFields(rowField: FormlyFieldConfig | undefined, inputType?: string, locked = false): void {
    this.configureRowColumns(rowField, (field, disabled) => this.configureDisabledState(field, disabled || locked));
    this.configureValueEditor(rowField, inputType);
  }

  /** A row whose value names a resource is edited with that resource's browser, not a text box. */
  private configureValueEditor(rowField: FormlyFieldConfig | undefined, inputType?: string): void {
    if (!rowField) return;
    const resource = resourceOf(inputType);
    this.rowResources.set(rowField, resource);
    const valueField = this.getColumnField(rowField, VALUE_COLUMN);
    if (!valueField || !resource) return;
    valueField.type = RESOURCE_VALUE_EDITOR;
    valueField.props = { ...(valueField.props ?? {}), resource };
  }

  /**
   * Formly keeps a row's config when the parameters are rebuilt from the code, and re-renders a
   * cell only when handed a new config. A row whose resource changed is therefore dropped, with
   * every row after it, so Formly rebuilds them from the template with the editor they now need.
   */
  private dropRowsWhoseResourceChanged(
    field: FormlyFieldConfig,
    rows: ReadonlyArray<{ inputType?: string } | undefined>
  ): void {
    const rowFields = field.fieldGroup ?? [];
    const firstChanged = rowFields.findIndex(
      (rowField, index) =>
        this.rowResources.has(rowField) && this.rowResources.get(rowField) !== resourceOf(rows[index]?.inputType)
    );
    if (firstChanged >= 0) rowFields.splice(firstChanged);
  }

  private configureRowColumns(
    rowField: FormlyFieldConfig | undefined,
    configureColumn: (field: FormlyFieldConfig | undefined, disabled: boolean) => void
  ): void {
    if (!rowField) return;

    this.fieldColumns.forEach(column => configureColumn(this.getColumnField(rowField, column), column.disabled));
  }

  private getChildField(rowField: FormlyFieldConfig | undefined, key: string): FormlyFieldConfig | undefined {
    return rowField?.fieldGroup?.find(fieldConfig => fieldConfig.key === key);
  }

  /** Sets Formly disabled metadata and keeps controls created later in sync through an onInit hook. */
  private configureDisabledState(field: FormlyFieldConfig | undefined, disabled: boolean): void {
    if (!field) return;

    this.setDisabledMetadata(field, disabled);

    if (this.disabledStateConfigured.get(field) === disabled) {
      this.applyDisabledState(field, disabled);
      return;
    }

    const previousOnInit = field.hooks?.onInit;
    field.hooks = {
      ...(field.hooks ?? {}),
      onInit: initializedField => {
        previousOnInit?.(initializedField);
        this.applyDisabledState(initializedField, disabled);
      },
    };

    this.disabledStateConfigured.set(field, disabled);
    this.applyDisabledState(field, disabled);
  }

  private setDisabledMetadata(field: FormlyFieldConfig | undefined, disabled: boolean): void {
    if (!field) return;

    field.props = { ...(field.props ?? {}), disabled };

    // Keep deprecated templateOptions in sync for existing Formly wrappers that still read it.
    (field as any).templateOptions = { ...((field as any).templateOptions ?? {}), disabled };
  }

  private applyDisabledState(field: FormlyFieldConfig, disabled: boolean): void {
    const control = field.formControl;
    if (!control) return;
    if (disabled) {
      control.disable({ emitEvent: false });
      this.keepLocked(control);
    } else {
      this.lockedControls.delete(control);
      control.enable({ emitEvent: false });
    }
  }

  /**
   * Hold a locked cell locked under any host. The operator property panel enables its whole form
   * group when it turns interactive (and again after a run), and formly mirrors a control's state
   * back into props.disabled, so a one-time disable did not survive there: the Name and Type cells
   * came back editable on the canvas, while the Form View, which never enables a group wholesale,
   * showed them locked. Angular tells a control's disabled-change hooks about every enable() and
   * disable(), whoever calls them (formly's mirror is such a hook), so the one registered here
   * disables the control again once a host has enabled it, and formly's mirror then records the
   * lock in props.disabled as well. The disabling waits for the microtask after the enable(): the
   * input's own value accessor is one more of these hooks, registered after this one when the cell
   * renders, and had the control been disabled from inside the enable()'s round of hooks, the
   * accessor would still have been told "enabled" last and the box would have shown editable over a
   * locked control. The disabling also announces itself (statusChanges), as the enable() did: the
   * cell's nz-input paints its disabled attribute from the status it was last told, and told only
   * "enabled" it would paint the box editable again at the next change detection. Registered once
   * per control, for good; a cell configured editable again leaves lockedControls
   * (applyDisabledState) and the hook lets it be.
   */
  private keepLocked(control: AbstractControl): void {
    this.lockedControls.add(control);
    // The hook is a FormControl's (a cell's control always is one; a group has none to register).
    if (!(control instanceof FormControl) || this.hookedControls.has(control)) return;
    this.hookedControls.add(control);
    control.registerOnDisabledChange((isDisabled: boolean) => {
      if (isDisabled) return;
      queueMicrotask(() => {
        if (control.enabled && this.lockedControls.has(control)) {
          control.disable();
        }
      });
    });
  }

  trackByParameterName = (index: number, parameter: any): string | number => {
    return parameter?.attribute?.attributeName ?? index;
  };
}
