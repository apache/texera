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
import { DATASET_INPUT_TYPE, MODEL_INPUT_TYPE } from "../../service/code-editor/ui-udf-parameters-parser.service";
import type { AttributeType } from "../../types/workflow-compiling.interface";
import { fieldOperatorID, renderedInFormView, renderedReadOnly } from "../../util/custom-formly-type";

type UiUdfParameterColumn = Readonly<{ label: string; key: string; parentKey?: string; disabled: boolean }>;

const VALUE_COLUMN: UiUdfParameterColumn = { label: "Value", key: "value", disabled: false };
const RESOURCE_VALUE_EDITOR = "resourcevalue";
const RESOURCE_INPUT_TYPES: ReadonlySet<string> = new Set([MODEL_INPUT_TYPE, DATASET_INPUT_TYPE]);

/** The resource a row's value names, or "" for free text. */
const resourceOf = (inputType?: string): string => (inputType && RESOURCE_INPUT_TYPES.has(inputType) ? inputType : "");

/** Renders inferred Python UDF UI parameters with editable values and locked name/type columns. */
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

  readonly fieldColumns: UiUdfParameterColumn[] = [
    VALUE_COLUMN,
    { label: "Name", key: "attributeName", parentKey: "attribute", disabled: true },
    { label: "Type", key: "attributeType", parentKey: "attribute", disabled: true },
  ];
  private shownColumns: UiUdfParameterColumn[] = this.fieldColumns;

  /**
   * The columns in view: the three fixed ones on the operator property panel; on the Form View,
   * what the author's overrides left of them (see columnsInView). Read live, on every check, not
   * once at populate: the form's hide lands on a row's sub-field after formly has built the row,
   * so a list fixed at populate still showed the hidden column. The same array instance comes back
   * while nothing changed, so the header's ngFor does not redraw on every check.
   */
  get columns(): UiUdfParameterColumn[] {
    const next = this.columnsInView(this.field ?? {});
    if (next.length !== this.shownColumns.length || next.some((column, i) => column !== this.shownColumns[i])) {
      this.shownColumns = next;
    }
    return this.shownColumns;
  }

  readonly addParameterTypeOptions: AttributeType[] = ["string", "integer", "long", "double", "boolean", "timestamp"];
  draftVisible = false;

  constructor(
    private workflowActionService: WorkflowActionService,
    private uiUdfParametersSyncService: UiUdfParametersSyncService,
    private notificationService: NotificationService
  ) {
    super();
  }

  /** Whether this is a Form View card rather than the operator property panel; see columnsInView. */
  get onFormView(): boolean {
    return renderedInFormView(this.field ?? {});
  }

  /**
   * Whether the table takes a new parameter here. On the operator property panel that is the
   * canvas's rule, as it always was: the workflow may be modified (write access, no run in flight).
   * On a Form View card it is the card's own rule: the card is not a reader's, which the form marks
   * on the field itself (props.disabled, its one lock on a card; see renderedReadOnly). A writer's
   * card is editable in and out of edit mode, as every value on it is (adding a parameter is adding
   * a row). The canvas's flag is not the card's rule: the Form View holds it off outside edit mode,
   * against structural edits through its page, and that hid the button on a card whose values could
   * still be edited. The mark decides rather than the table's own control, which does not answer the
   * question: formly disables the leaf controls under a disabled field, not the array itself, so a
   * reader's table with no parameters yet reads as enabled (a reader's table with rows reads as
   * disabled only because every row's controls are).
   */
  get editable(): boolean {
    if (!this.onFormView) {
      return this.workflowActionService.checkWorkflowModificationEnabled();
    }
    return !renderedReadOnly(this.field);
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
  }

  /**
   * On the Form View the author's per-sub-field overrides (#8438) land on every row's sub-fields: a
   * hide as `hide` for a reader (an author sees the hidden column faded, still in view), a rename as
   * `props.authorName` (the name the author saved, or is typing: the label wrapper writes it to every
   * row and blanks `props.label`). The fixed headers papered over both -- the formly labels are
   * hidden here, so a renamed column kept its old name, and a hidden one kept its header over an
   * empty cell (#8763). Which columns are in view is decided here, from the first row: the rows all
   * share one override per path, and formly builds them on demand from a template this widget never
   * sees decorated. The three fixed columns come back while the draft row is open: that row names
   * and types the parameter being added, so it needs all three whatever the author hid, and a
   * filtered header over its three cells put the name box under the wrong heading (Copilot on
   * #8846). They are also the answer before formly has built a row, which is every table whose UDF
   * declares no parameters yet. Read live (see columns); the header text too, see columnLabel. On
   * the operator property panel the three columns stay.
   */
  private columnsInView(field: FormlyFieldConfig): UiUdfParameterColumn[] {
    const decorated = field.fieldGroup?.[0];
    if (!renderedInFormView(field) || !decorated || this.draftVisible) {
      return this.fieldColumns;
    }
    // Out of view when its own sub-field is hidden, or the group it sits in is: formly hides the
    // children with the group but marks only the group.
    return this.fieldColumns.filter(column => {
      const group = column.parentKey ? this.getChildField(decorated, column.parentKey) : undefined;
      return !group?.hide && !this.getColumnField(decorated, column)?.hide;
    });
  }

  /**
   * A column's header. On the Form View the author's name for it, read from the first row on every
   * check so a rename being typed shows at once (the form puts a saved name on every row's sub-field
   * as `props.authorName`, and the author's name box writes there while typing); the fixed label
   * otherwise -- not the schema's own title, which the panel does not show either -- and on the
   * operator property panel always.
   */
  columnLabel(column: UiUdfParameterColumn): string {
    const first = this.field?.fieldGroup?.[0];
    if (!first || !this.onFormView) {
      return column.label;
    }
    const named = this.getColumnField(first, column)?.props?.["authorName"];
    return typeof named === "string" && named ? named : column.label;
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
    if (disabled) field.formControl?.disable({ emitEvent: false });
    else field.formControl?.enable({ emitEvent: false });
  }

  trackByParameterName = (index: number, parameter: any): string | number => {
    return parameter?.attribute?.attributeName ?? index;
  };
}
