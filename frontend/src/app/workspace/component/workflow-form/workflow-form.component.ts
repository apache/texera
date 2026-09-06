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

import { ChangeDetectorRef, Component, ElementRef, HostListener, OnDestroy, OnInit } from "@angular/core";
import { CommonModule, DatePipe } from "@angular/common";
import { FormGroup, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
import { FormlyJsonschema } from "@ngx-formly/core/json-schema";
import { ActivatedRoute, Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { NzIconModule } from "ng-zorro-antd/icon";
import { UserIconComponent } from "../../../dashboard/component/user/user-icon/user-icon.component";
import { cloneDeep } from "lodash-es";
import { forkJoin, Subject } from "rxjs";
import { debounceTime, takeUntil } from "rxjs/operators";

import { USER_WORKFLOW, USER_WORKSPACE } from "../../../app-routing.constant";
import { FormFieldBinding, Workflow, WorkflowContent } from "../../../common/type/workflow";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { UserService } from "../../../common/service/user/user.service";
import { DynamicSchemaService } from "../../service/dynamic-schema/dynamic-schema.service";
import { customFormlyFieldType, CANVAS_ONLY_FORMLY_TYPES } from "../../util/custom-formly-type";
import { WorkflowCompilingService } from "../../service/compile-workflow/workflow-compiling.service";
import { ExecuteWorkflowService, FORM_DEBOUNCE_TIME_MS } from "../../service/execute-workflow/execute-workflow.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { FormBindingService, ResolvedField } from "../../service/form-binding/form-binding.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { WorkflowConsoleService } from "../../service/workflow-console/workflow-console.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { Point } from "../../types/workflow-common.interface";
import { WorkflowEditorComponent } from "../workflow-editor/workflow-editor.component";
import { MiniMapComponent } from "../workflow-editor/mini-map/mini-map.component";
import { CoeditorUserIconComponent } from "../menu/coeditor-user-icon/coeditor-user-icon.component";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { SAVE_DEBOUNCE_TIME_IN_MS } from "../workspace.component";

/**
 * One rendered input: the resolved binding plus the operator's own formly field for that property.
 * Building the field from the operator's JSON schema (not guessing from the value) is what gives a
 * file its picker and an attribute its column dropdown.
 */
interface RenderedField {
  resolved: ResolvedField;
  fields: FormlyFieldConfig[];
  form: FormGroup;
  model: Record<string, unknown>;
}

/**
 * The Form View: a second way to use a workflow. On top of the title-bar frame and the collapsible
 * read-only workflow preview, this PR renders the inputs an author exposed -- each as its
 * operator's own formly field, so a file property gets the real picker and an attribute a column
 * dropdown -- and writes a filled-in value straight back to its operator, the same edit the canvas
 * makes, with each sub-field of a nested or repeated property renamed and hidden as the author set
 * it up. Running the workflow and showing results are added by later PRs. A view, not a new object:
 * it opens the same workflow the canvas does.
 */
@UntilDestroy()
@Component({
  selector: "texera-workflow-form",
  templateUrl: "./workflow-form.component.html",
  styleUrls: ["./workflow-form.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    FormlyModule,
    NzAvatarModule,
    NzIconModule,
    UserIconComponent,
    WorkflowEditorComponent,
    MiniMapComponent,
    CoeditorUserIconComponent,
  ],
})
export class WorkflowFormComponent implements OnInit, OnDestroy {
  public wid?: number;
  public workflowName = "";
  public loading = true;
  /** "Saved at …", worded and formatted exactly as on the operator canvas. */
  public autoSaveState = "";
  /** Write access: only then does a filled-in value write back, and only then does the page save. */
  public canEdit = false;

  /** The exposed inputs, resolved against the live graph, and the formly field built for each. */
  private parameters: ResolvedField[] = [];
  public rendered: RenderedField[] = [];
  /** Torn down and replaced whenever the form is rebuilt, so an old field's write-back stops. */
  private formsRebuilt = new Subject<void>();

  /** The collapsible workflow preview: closed until the reader opens it. */
  public workflowOpen = false;
  /** The embedded canvas is built the first time the strip opens, never while collapsed. */
  public workflowEverOpened = false;

  /** Set on teardown so deferred callbacks stop touching a view that is gone. */
  private destroyed = false;

  /**
   * Operator positions as loaded, kept only as a fallback: a save writes the live positions
   * from the shared model (a co-editor's drags included), and falls back to this snapshot, then
   * origin, if the live map is ever missing an operator -- so a save never drops an operator's
   * position, and never overwrites a co-editor's move with a stale one.
   */
  private storedPositions: { [operatorID: string]: Point } = {};

  constructor(
    // Public for the template: shows the same live collaborator avatars as the canvas.
    public coeditorPresenceService: CoeditorPresenceService,
    private route: ActivatedRoute,
    private router: Router,
    private workflowActionService: WorkflowActionService,
    private workflowPersistService: WorkflowPersistService,
    private operatorMetadataService: OperatorMetadataService,
    private formBindingService: FormBindingService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private notificationService: NotificationService,
    private userService: UserService,
    private formlyJsonschema: FormlyJsonschema,
    private cdr: ChangeDetectorRef,
    // Injected for its side effect: it fills its map from the operator-add stream, so it has to
    // exist before the workflow loads or every operator arrives unregistered and anything asking
    // for a schema later throws. It also carries the per-instance schema (upstream column names)
    // that turns an attribute box into a dropdown.
    private dynamicSchemaService: DynamicSchemaService,
    // Injected for its side effect: it compiles on graph changes and writes column names into each
    // operator's dynamic schema. Nothing else on this page injects it, so without this line it
    // never runs and an attribute input stays a plain text box.
    private workflowCompilingService: WorkflowCompilingService,
    private computingUnitStatusService: ComputingUnitStatusService,
    private workflowConsoleService: WorkflowConsoleService,
    private host: ElementRef<HTMLElement>,
    private datePipe: DatePipe,
    private config: GuiConfigService
  ) {}

  ngOnInit(): void {
    const wid = Number(this.route.snapshot.params.id);
    if (!Number.isFinite(wid)) {
      void this.router.navigate([USER_WORKFLOW]);
      return;
    }
    this.wid = wid;
    this.load(wid);

    // Attribute boxes become dropdowns only after compilation writes the column enums into each
    // operator's dynamic schema -- which lands after these cards were built. Rebuild on the
    // compilation-state stream, a ReplaySubject(1) so a late subscriber (this page reloads fresh
    // on every Canvas<->Form switch) gets the current state at once. Skip it while someone is
    // typing, so a rebuild does not throw away a half-entered value under the cursor.
    this.workflowCompilingService
      .getCompilationStateInfoChangedStream()
      .pipe(debounceTime(FORM_DEBOUNCE_TIME_MS), untilDestroyed(this))
      .subscribe(() => {
        if (this.isTypingInTheForm()) {
          return;
        }
        this.readConfig();
      });

    // Exposing or un-exposing a property in the panel changes the definition; the inputs above have
    // to follow at once, which is the whole point of editing them side by side. Today this fires for
    // this client's own edits; once #8351 moves formBinding into the shared model it also fires for
    // a co-editor's -- so, like the compilation path, skip the rebuild while the reader is typing, or
    // a remote change would throw away a half-entered value under the cursor.
    this.workflowActionService.formBindingChanged$.pipe(untilDestroyed(this)).subscribe(() => {
      if (this.isTypingInTheForm()) {
        return;
      }
      this.readConfig();
      this.cdr.detectChanges();
    });
  }

  private load(wid: number): void {
    // With the feature off the form does not exist: hand straight to the operator canvas
    // without loading anything, so a request that then fails cannot strand the visitor on
    // an error instead of the page they would have gotten.
    if (!this.config.env.formViewEnabled) {
      void this.router.navigate([USER_WORKSPACE, String(wid)], { replaceUrl: true });
      return;
    }
    this.workflowActionService.resetAsNewWorkflow();
    forkJoin({
      metadata: this.operatorMetadataService.getOperatorMetadata(),
      workflow: this.workflowPersistService.retrieveWorkflow(wid),
    })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ workflow }) => {
          // With the flag on, the form renders for any workflow: default_view only decides
          // which view a workflow lands on by default, not whether the form is reachable
          // (settled on #8011). Gating the form on default_view here would quietly reintroduce
          // a per-workflow switch -- and bounce a later PR's canvas-to-form switch straight
          // back for any canvas-default workflow.
          this.workflowName = workflow.name;
          this.storedPositions = { ...(workflow.content?.operatorPositions ?? {}) };
          this.canEdit = !workflow.readonly;
          this.workflowActionService.setNewSharedModel(wid, this.userService.getCurrentUser());
          this.workflowActionService.reloadWorkflow(workflow);
          // The workflow is shown, not edited, from here: dragging operators around or
          // deleting them belongs to the operator canvas.
          this.applyEditability();
          this.refreshSavedState();
          this.later(() => this.adjustWorkflowNameWidth(), 0);
          this.readConfig();
          this.registerMetadataRefresh();
          this.registerAutoPersist();
          this.loading = false;
          this.cdr.detectChanges();
        },
        // The load can fail for many reasons (no access, a network or server error, the
        // metadata call): a neutral message covers them without claiming it was permissions.
        error: () => {
          this.notificationService.error("Unable to open this workflow.");
          void this.router.navigate([USER_WORKFLOW]);
        },
      });
  }

  /**
   * Show the workflow rather than edit it: the graph shape and its properties are read-only
   * on this page. A later PR's authoring mode makes properties editable with write access.
   */
  private applyEditability(): void {
    this.workflowActionService.disableWorkflowModification();
  }

  // ---------------------------------------------------------------------------
  // Inputs: the exposed properties, rendered as their operators' own fields
  // ---------------------------------------------------------------------------

  /** Whether the cursor is currently inside one of this page's inputs. */
  private isTypingInTheForm(): boolean {
    const active = document.activeElement as HTMLElement | null;
    if (!active || !this.host.nativeElement.contains(active)) {
      return false;
    }
    return ["INPUT", "TEXTAREA", "SELECT"].includes(active.tagName) || active.isContentEditable;
  }

  private readConfig(): void {
    this.parameters = this.formBindingService.resolveFields();
    this.buildForm();
  }

  /**
   * Build the form from the operators' JSON schemas (FormlyJsonschema), keeping the one field per
   * exposed property. Each input gets its own form keyed by binding id.
   */
  private buildForm(): void {
    this.formsRebuilt.next();
    this.rendered = this.visibleFields
      .map(field => this.renderField(field))
      .filter((r): r is RenderedField => r !== undefined);
  }

  private renderField(resolved: ResolvedField): RenderedField | undefined {
    const { binding } = resolved;
    const schema = this.operatorSchemaFor(binding.operatorID);
    if (!schema) {
      return undefined;
    }
    const operator = this.workflowActionService.getTexeraGraph().getOperator(binding.operatorID);
    const operatorType = operator?.operatorType;
    const full = this.formlyJsonschema.toFieldConfig(cloneDeep(schema) as never, {
      map: (mapped, source) => {
        // Render the exact custom widget the operator property panel would (file/model/dataset
        // pickers, image/audio uploaders, ...), shared via customFormlyFieldType so an exposed
        // property shows its real control instead of degrading to a text box.
        const customType = customFormlyFieldType({
          key: mapped.key,
          operatorType,
          description: (source as { description?: string })?.description,
          currentType: mapped.type,
        });
        // Canvas-only widgets (code editor, drag-reorder) do not work here; an older workflow may
        // already carry one, so leave it to formly's default editable control rather than a widget
        // that cannot function on a form.
        if (customType && !CANVAS_ONLY_FORMLY_TYPES.has(customType)) {
          mapped.type = customType;
        }
        return mapped;
      },
    });
    const source = (full.fieldGroup ?? []).find(child => child.key === binding.propertyKey);
    if (!source) {
      return undefined;
    }

    const field = cloneDeep(source);
    // The schema's own title ("Attributes", "Limit", "File") -- the reader's title when unnamed.
    // Falls back to this, not the lower-camel key ("fileName"), which would read inconsistently.
    const schemaLabel = (source.props?.label as string) || binding.propertyKey;
    field.key = binding.id;
    field.props = {
      ...(field.props ?? {}),
      label: binding.displayName || schemaLabel,
    };

    const form = new FormGroup({});
    // Seed the model with the operator's other properties as read-only context, not just this
    // input's own value: some custom widgets read a sibling to decide what to show -- the
    // HuggingFace model picker reads `task` to load the right models and label the field. Only
    // this binding's value is ever written back (see below); the context is never persisted, and
    // it is cloned so a widget that mutates it cannot reach through to the real operator.
    const model: Record<string, unknown> = {
      ...cloneDeep(operator?.operatorProperties ?? {}),
      [binding.id]: cloneDeep(resolved.value),
    };
    if (this.canEdit) {
      form.valueChanges
        .pipe(debounceTime(FORM_DEBOUNCE_TIME_MS), takeUntil(this.formsRebuilt), untilDestroyed(this))
        .subscribe(() => {
          // Formly emits the schema's empty default while building the control, before any edit;
          // writing that back silently wiped the operator's real value (both views edit one
          // workflow). So only accept a dirtied form, or a value that differs from the operator's
          // without being emptier (some controls set values without marking dirty).
          const next = model[binding.id];
          const current = this.formBindingService.readValue(binding.operatorID, binding.propertyKey);
          const isEmpty = (v: unknown) => v === undefined || v === null || v === "";
          const unchanged = JSON.stringify(next ?? null) === JSON.stringify(current ?? null);
          if (unchanged || (!form.dirty && isEmpty(next) && !isEmpty(current))) {
            return;
          }
          // Write straight onto the operator (the same edit the canvas makes) and refresh this
          // card's snapshot, which the template reads.
          this.formBindingService.writeValue(binding, next);
          this.parameters = this.formBindingService.resolveFields();
          const refreshed = this.parameters.find(p => p.binding.id === binding.id);
          const card = this.rendered.find(r => r.resolved.binding.id === binding.id);
          if (refreshed && card) {
            card.resolved = refreshed;
          }
          this.cdr.detectChanges();
        });
    } else {
      // A read-only viewer sees the author's values and can run with them, but cannot change them.
      // Disable at the field level, not with form.disable(): formly builds its controls into the
      // form after this, and a FormGroup disabled while still empty does not disable controls added
      // later (it re-enables itself), so the input stayed editable. props.disabled is what formly
      // honours, and it cascades to a nested property's sub-fields. No write-back is wired either.
      field.props = { ...(field.props ?? {}), disabled: true };
    }

    this.applyFieldOverrides(field, binding);
    return { resolved, fields: [field], form, model };
  }

  /**
   * The template for one row of a repeated section. formly's `fieldArray` may be the template
   * object or a function that builds one per row; resolve both so an array property's sub-fields
   * are reachable (treating the function case as a leaf hid them). @internal, exported for tests.
   */
  public static arrayItemOf(node: FormlyFieldConfig): FormlyFieldConfig | undefined {
    const fa = node.fieldArray;
    if (!fa) {
      return undefined;
    }
    if (typeof fa !== "function") {
      return fa;
    }
    try {
      return fa(node);
    } catch {
      // A builder that needs more context than we can give it tells us nothing about the row's
      // shape; better to list no sub-fields than to guess at them.
      return undefined;
    }
  }

  /**
   * The override path for a child field: the parent path joined with the child's key, but array
   * indices are dropped so one override entry covers every row of a repeated section. @internal,
   * exported for tests.
   */
  public static childPath(parent: string, key: unknown): string {
    if (typeof key !== "string" || key === "" || /^\d+$/.test(key)) {
      return parent;
    }
    return parent ? parent + "." + key : key;
  }

  /**
   * Walk the field and its sub-fields, dropping the operator schema's own per-field descriptions
   * (author notes about the operator, not guidance to a form reader) and applying the author's
   * stored per-sub-field overrides (rename, hide), keyed by field path. A repeated section builds
   * its row template on demand, so its builder is wrapped to decorate every row formly ever makes.
   */
  private applyFieldOverrides(field: FormlyFieldConfig, binding: FormFieldBinding): void {
    const walk = (node: FormlyFieldConfig, path: string): void => {
      // Drop the schema's own description on every field, nested ones included: on this page the
      // one piece of guidance is the help text the form's author writes, rendered once by the card.
      node.props = { ...(node.props ?? {}), description: "" };
      // Apply the author's stored overrides so a reader sees each sub-field renamed and hidden as
      // set up. The root (path "") carries the binding's own displayName, set in renderField.
      if (path) {
        const override = binding.overrides?.[path] ?? {};
        if (override.displayName) {
          node.props = { ...(node.props ?? {}), label: override.displayName };
        }
        if (override.hidden) {
          node.hide = true;
          // Hidden means "not shown", not "cleared". Formly 7's resetFieldOnHide extra defaults to
          // true, so a field that renders hidden has its value stripped from the model -- and this
          // card writes the whole nested object back, so that strip would delete the author's pinned
          // value for the hidden sub-field the moment a writer opens the form. Opt this field out so
          // its value survives, matching FormFieldOverride.hidden's contract (the value still
          // applies; it is only hidden).
          node.resetOnHide = false;
        }
      }
      // A repeated section may build its row template on demand, once per row. Decorating the
      // object it returns is pointless -- the next row gets a fresh one. Wrap the builder instead,
      // so every row formly ever creates comes out decorated.
      if (typeof node.fieldArray === "function") {
        const build = node.fieldArray;
        node.fieldArray = (f: FormlyFieldConfig) => {
          const row = build(f);
          // Walk what is INSIDE each row, never the row container itself: the container carries the
          // array property's own name, so decorating it as a root (path "") printed the group title
          // a second time above the rows. Its sub-fields keep their own key paths, the same ones
          // their overrides are stored under.
          const children = row.fieldGroup ?? [];
          if (children.length === 0) {
            // A scalar array (a list of strings): the builder returns a leaf row with no sub-fields,
            // so decorate the row itself, mirroring the leaf case of the non-function branch below.
            walk(row, path);
          } else {
            // An object row: not walked as a root (that reprints the array's group title), but its
            // own schema description (the items.description) still renders once per row via the
            // field wrapper's nzExtra, so drop just that -- the description-removal the walk does for
            // every other field, minus the title-reprinting root treatment.
            row.props = { ...(row.props ?? {}), description: "" };
          }
          for (const child of children) {
            walk(child, WorkflowFormComponent.childPath(path, child.key));
          }
          return row;
        };
        return;
      }
      const arrayItem = WorkflowFormComponent.arrayItemOf(node);
      const children = node.fieldGroup ?? arrayItem?.fieldGroup ?? [];
      for (const child of children) {
        walk(child, WorkflowFormComponent.childPath(path, child.key));
      }
      // A scalar array (e.g. a list of strings) has a row template with no sub-fields of its own;
      // decorate it directly so its schema description is dropped like every other field's.
      if (arrayItem && !arrayItem.fieldGroup) {
        walk(arrayItem, path);
      } else if (arrayItem) {
        // A static object-array template: its sub-fields are walked above, but the template
        // container's own items.description still renders once per row, so drop just that (not
        // walking it as a root, which would reprint the array's group title).
        arrayItem.props = { ...(arrayItem.props ?? {}), description: "" };
      }
    };
    walk(field, "");
  }

  private operatorSchemaFor(operatorID: string): object | undefined {
    const graph = this.workflowActionService.getTexeraGraph();
    if (!graph.hasOperator(operatorID)) {
      return undefined;
    }
    try {
      // Prefer the per-instance schema: it carries the upstream column names, so an attribute
      // picker renders as a dropdown of real columns rather than a text box.
      return this.dynamicSchemaService.getDynamicSchema(operatorID).jsonSchema;
    } catch {
      try {
        return this.operatorMetadataService.getOperatorSchema(graph.getOperator(operatorID).operatorType).jsonSchema;
      } catch {
        return undefined;
      }
    }
  }

  /**
   * The inputs a reader is offered. Broken bindings (the operator was deleted, or the property key
   * no longer exists) are left out, since filling one in could not affect a run; the author's view
   * of them, to repair them, is added by the authoring PR.
   */
  public get visibleFields(): ResolvedField[] {
    return this.parameters.filter(field => !field.brokenReason);
  }

  public trackByRendered(_: number, rendered: RenderedField): string {
    return rendered.resolved.binding.id;
  }

  /** Open or close the workflow preview; opening it builds the canvas the first time. */
  public toggleWorkflow(): void {
    this.workflowOpen = !this.workflowOpen;
    if (this.workflowOpen) {
      this.openWorkflowStrip();
    }
  }

  /**
   * Reveal the strip, then build the canvas a frame later (so JointJS measures the strip's
   * real size, not a zero-sized frame that misroutes links), then centre the graph a frame
   * after that so the fit runs against a canvas that exists. The editor keeps its own paper
   * sized via its container ResizeObserver, so nothing more is needed here.
   *
   * Each deferred step rechecks `workflowOpen`: a reader who opens then immediately collapses
   * the strip must not have the children mounted into a now-hidden (0-sized) body -- the
   * embedded mini-map is a fixed-size widget with no resize observer, so mounting it collapsed
   * would leave it blank on the next open.
   */
  private openWorkflowStrip(): void {
    this.later(() => {
      if (!this.workflowOpen) {
        return;
      }
      this.workflowEverOpened = true;
      this.cdr.detectChanges();
      this.later(() => {
        if (this.workflowOpen) {
          this.workflowActionService.getTexeraGraph().triggerCenterEvent();
        }
      });
    });
  }

  /**
   * Size the name field to its text, the way the operator canvas does, so what follows
   * it starts at the same place in both views instead of after a fixed-width box.
   */
  private adjustWorkflowNameWidth(): void {
    const input = this.host.nativeElement.querySelector<HTMLInputElement>("input.wf-name");
    if (!input) {
      return;
    }
    /* v8 ignore start -- font-metrics DOM measuring; jsdom has no layout */
    const probe = document.createElement("span");
    probe.style.visibility = "hidden";
    probe.style.position = "absolute";
    probe.style.whiteSpace = "pre";
    probe.style.font = getComputedStyle(input).font;
    probe.textContent = input.value || input.placeholder;
    document.body.appendChild(probe);
    input.style.width = `${Math.min(probe.offsetWidth + 20, 800)}px`;
    document.body.removeChild(probe);
    /* v8 ignore stop */
  }

  private refreshSavedState(): void {
    const lastModified = this.workflowActionService.getWorkflowMetadata()?.lastModifiedTime;
    this.autoSaveState =
      lastModified === undefined
        ? ""
        : "Saved at " +
          (this.datePipe.transform(
            lastModified,
            "MM/dd/yyyy HH:mm:ss",
            Intl.DateTimeFormat().resolvedOptions().timeZone,
            "en"
          ) ?? "");
  }

  /**
   * Renaming here is the same edit as renaming on the operator canvas: commit the name and
   * save. The title bar itself -- the read-back (normalised) name and its width -- is
   * refreshed from the metadata subscription below, the single place this page's own rename
   * and a co-editor's both flow through.
   */
  public onRenameWorkflow(): void {
    this.workflowActionService.setWorkflowName(this.workflowName);
    this.save();
  }

  /**
   * Keep the title bar in step with the workflow's metadata, exactly as the operator canvas
   * does: a rename or a save -- this page's own or a co-editor's -- refreshes the name, its
   * width, and the "Saved at ..." state from one place, so the two views never drift apart.
   */
  private registerMetadataRefresh(): void {
    this.workflowActionService
      .workflowMetaDataChanged()
      // The same 100ms the operator canvas debounces its title-bar refresh by.
      .pipe(debounceTime(100), untilDestroyed(this))
      .subscribe(() => {
        this.workflowName = this.workflowActionService.getWorkflowMetadata()?.name ?? "";
        this.later(() => this.adjustWorkflowNameWidth(), 0);
        this.refreshSavedState();
      });
  }

  /**
   * Switch to the operator canvas with a full page load, not a route. The two views share
   * root-level singletons (the graph, the Yjs shared model, the CU connection); handing
   * over in-process left the old state attached -- undraggable operators, a ghost coeditor
   * of yourself, broken runs. A fresh document is the reliable handover.
   */
  public openRegularCanvas(): void {
    this.save();
    /* v8 ignore start -- full-document navigation; jsdom cannot navigate */
    window.location.href = `${USER_WORKSPACE}/${this.wid}`;
    /* v8 ignore stop */
  }

  /**
   * Save the same way the operator canvas does. Both views edit one workflow, so the
   * form has to write through the same debounced persist -- otherwise an author's
   * setup, or a value someone filled in, would be gone on the next visit.
   */
  private registerAutoPersist(): void {
    this.workflowActionService
      .workflowChanged()
      .pipe(debounceTime(SAVE_DEBOUNCE_TIME_IN_MS), untilDestroyed(this))
      .subscribe(() => this.save());
  }

  /**
   * Save the workflow this page opened, and only that one. The persist endpoint creates a
   * workflow when the payload has no id, so saving whatever the graph holds would spawn
   * stray "Untitled workflow" rows when the page is left before its workflow loaded.
   */
  private save(): void {
    // A read-only viewer can open and run the form (execution is gated on computing-unit access,
    // not workflow access) but must never persist: every such save is a guaranteed 403 that would
    // spam "Could not save" on each debounce. Their inputs are non-editable, so nothing is lost.
    if (!this.canEdit) {
      return;
    }
    if (!this.userService.isLogin() || !this.workflowPersistService.isWorkflowPersistEnabled()) {
      return;
    }
    const workflow = this.workflowActionService.getWorkflow();
    if (workflow.wid === undefined || workflow.wid !== this.wid) {
      return;
    }
    const preserved: Workflow = {
      ...workflow,
      content: { ...workflow.content, operatorPositions: this.positionsToSave(workflow.content) },
    };
    // On the way out the subscription must NOT be tied to this component: ngOnDestroy
    // calls save(), and untilDestroyed would tear the subscription down as part of the
    // very same destroy sequence, aborting the request that was the point of the call.
    const persist = this.workflowPersistService.persistWorkflow(preserved);
    // The `destroyed` branch deliberately omits untilDestroyed (see above); the persist call
    // is a one-shot HTTP request that completes on its own, so it needs no teardown operator.
    // eslint-disable-next-line rxjs-angular/prefer-takeuntil
    (this.destroyed ? persist : persist.pipe(untilDestroyed(this))).subscribe({
      // Feed the saved workflow back, exactly as the operator canvas does: this advances
      // lastModifiedTime (and the normalised name), and the metadata subscription then repaints
      // the title bar -- so "Saved at ..." moves past the moment the page opened.
      next: updatedWorkflow => this.workflowActionService.setWorkflowMetadata(updatedWorkflow),
      // A save that fails silently is the worst thing this page can do: the author walks
      // away believing the form they just built is stored.
      error: () => this.notificationService.error("Could not save. Your latest changes are not stored yet."),
    });
  }

  /**
   * A position for every operator: the live one from the shared model (what a co-editor's drag
   * has set), else the load-time snapshot, else origin. Preferring live saves what is current
   * rather than overwriting moves with a stale copy; the fallbacks keep the guarantee that every
   * operator has a position, since loading throws on one that does not.
   */
  private positionsToSave(content: WorkflowContent): { [operatorID: string]: Point } {
    const positions: { [operatorID: string]: Point } = {};
    for (const operator of content.operators) {
      positions[operator.operatorID] = content.operatorPositions?.[operator.operatorID] ??
        this.storedPositions[operator.operatorID] ?? { x: 0, y: 0 };
    }
    return positions;
  }

  /**
   * Run after the current frame (or a delay), unless the page is gone by then: these callbacks
   * touch the view, and detectChanges on a destroyed view throws -- reachable by navigating away
   * while the name field is waiting to be measured, or the preview canvas to be built.
   */
  private later(fn: () => void, delayMs?: number): void {
    const run = () => {
      if (!this.destroyed) {
        fn();
      }
    };
    if (delayMs === undefined) {
      requestAnimationFrame(run);
    } else {
      setTimeout(run, delayMs);
    }
  }

  /**
   * Tear down exactly what the operator canvas tears down: both views drive the same
   * singleton services, so anything left bound here follows the user to the next page
   * (the symptom was a frozen canvas after a visit -- the old shared model still attached).
   * On the way out, save once more so a last edit is not lost.
   */
  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    this.destroyed = true;
    this.save();
    this.workflowActionService.clearWorkflow();
    this.computingUnitStatusService.disconnect();
    this.executeWorkflowService.resetExecutionAndWorkers();
    this.workflowConsoleService.clearConsoleMessages();
    this.workflowResultService.clearResults();
  }
}
