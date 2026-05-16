/**
 * AI Wizard Panel — a sibling dock to the agent-panel. Hosts a 4-step wizard
 * that produces a Texera workflow and applies it to the canvas via
 * WorkflowActionService.reloadWorkflow().
 *
 * Mounted in workspace.component.html as <texera-ai-wizard-panel>.
 * Design-doc §5 (P0 wizard + chat) and §4.2 (data profiler).
 */

import { CommonModule } from "@angular/common";
import { Component, HostListener, OnDestroy, OnInit } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzResizableDirective, NzResizeEvent, NzResizeHandlesComponent } from "ng-zorro-antd/resizable";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NgClass } from "@angular/common";

import { NzModalService } from "ng-zorro-antd/modal";
import { firstValueFrom } from "rxjs";

import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { AgentService, ModelType } from "../../../service/agent/agent.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { OperatorMetadata, OperatorSchema } from "../../../types/operator-schema.interface";
import { OperatorPredicate } from "../../../types/workflow-common.interface";
import { WorkflowGeneratorService, GeneratedWorkflow } from "../../../service/ai-wizard/workflow-generator.service";
import { DataProfilerService } from "../../../service/ai-wizard/data-profiler.service";
import { findMissingRequiredPaths } from "../../../service/ai-wizard/prompt-builders";
import { DatasetService } from "../../../../dashboard/service/user/dataset/dataset.service";
import { DatasetSelectionModalComponent } from "../../dataset-selection-modal/dataset-selection-modal.component";
import { DEFAULT_GUARDRAILS, getGuardrailsPrompt } from "../../../service/ai-wizard/data/guardrails";
import { FRAMEWORKS } from "../../../service/ai-wizard/data/frameworks";
import { DKNET_DATASETS } from "../../../service/ai-wizard/data/dknet-datasets";
import {
  AnalysisGoal,
  AttemptLog,
  DataSource,
  DknetDataset,
  ScientificFramework,
  WizardState,
} from "../../../service/ai-wizard/types";
import { WorkflowContent } from "../../../../common/type/workflow";

const ANALYSIS_GOALS: AnalysisGoal[] = ["EDA", "Predictive Modeling", "Data Cleaning", "NLP", "Custom"];
const DATA_SOURCES: DataSource[] = ["Existing Dataset", "dkNET Dataset"];
const FRAMEWORK_NAMES: ScientificFramework[] = ["CRISP-DM", "SEMMA", "KDD", "Custom"];

const GOAL_DESCRIPTIONS: Record<AnalysisGoal, string> = {
  EDA: "Exploratory Data Analysis - distributions, correlations, patterns",
  "Predictive Modeling": "Build and evaluate ML models",
  "Data Cleaning": "Clean, transform, and prepare data",
  NLP: "Natural Language Processing on text",
  Custom: "Describe your own analysis goal (power-user free text)",
};

const FRAMEWORK_DESCRIPTIONS: Record<ScientificFramework, string> = {
  "CRISP-DM": "Business → Data → Preparation → Modeling → Evaluation",
  SEMMA: "Sample → Explore → Modify → Model → Assess",
  KDD: "Selection → Preprocessing → Transformation → Mining → Interpretation",
  Custom: "Write your own methodology and domain-specific guidance from scratch",
};

@UntilDestroy()
@Component({
  selector: "texera-ai-wizard-panel",
  templateUrl: "ai-wizard-panel.component.html",
  styleUrls: ["ai-wizard-panel.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    NgClass,
    CdkDrag,
    CdkDragHandle,
    NzButtonComponent,
    NzIconDirective,
    NzResizableDirective,
    NzResizeHandlesComponent,
    NzTooltipDirective,
    NzMenuDirective,
    NzMenuItemComponent,
  ],
})
export class AiWizardPanelComponent implements OnInit, OnDestroy {
  protected readonly window = window;
  private static readonly MIN_W = 480;
  private static readonly MIN_H = 540;

  // Panel chrome
  width = 0;
  height = Math.max(AiWizardPanelComponent.MIN_H, window.innerHeight * 0.75);
  dragPosition = { x: 0, y: 0 };
  isDocked = true;
  private resizeRaf = -1;

  // Wizard data
  readonly goals = ANALYSIS_GOALS;
  readonly dataSources = DATA_SOURCES;
  readonly frameworks = FRAMEWORK_NAMES;
  readonly dknetDatasets = DKNET_DATASETS;
  readonly goalDescriptions = GOAL_DESCRIPTIONS;
  readonly frameworkDescriptions = FRAMEWORK_DESCRIPTIONS;

  state: WizardState = {
    step: 1,
    guardrails: DEFAULT_GUARDRAILS.map(g => ({ ...g })),
  };

  // Available LLM models (fetched from /api/models via AgentService).
  availableModels: ModelType[] = [];

  // Live operator catalog for the review panel.
  operatorCatalog: OperatorMetadata | null = null;

  // Whether the user has reviewed + approved before Apply (Stage B gate).
  reviewApproved = false;

  // Generation results
  generatedWorkflow: WorkflowContent | null = null;
  whyExplanations: Record<string, string> = {};
  attempts: AttemptLog[] = [];
  isGenerating = false;
  isModifying = false;
  generationError: string | null = null;
  modificationError: string | null = null;
  nlInstruction = "";
  editHistory: string[] = [];

  constructor(
    private generator: WorkflowGeneratorService,
    private profiler: DataProfilerService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private agentService: AgentService,
    private operatorMetadataService: OperatorMetadataService,
    private modalService: NzModalService,
    private datasetService: DatasetService
  ) {}

  ngOnInit(): void {
    const savedW = Number(localStorage.getItem("ai-wizard-width"));
    const savedH = Number(localStorage.getItem("ai-wizard-height"));
    const wasDocked = localStorage.getItem("ai-wizard-docked");
    if (wasDocked === "false" && !isNaN(savedW) && savedW >= AiWizardPanelComponent.MIN_W) {
      this.width = savedW;
    }
    if (!isNaN(savedH) && savedH >= AiWizardPanelComponent.MIN_H) {
      this.height = savedH;
    }
    // Load available LLM models from Texera's LiteLLM proxy.
    this.agentService
      .fetchModelTypes()
      .pipe(untilDestroyed(this))
      .subscribe(models => {
        this.availableModels = models;
        const saved = localStorage.getItem("ai-wizard-model");
        if (saved && models.some(m => m.id === saved)) {
          this.state.model = saved;
        } else if (models.length > 0 && !this.state.model) {
          this.state.model = models[0].id;
        }
      });
    // Cache the operator catalog for the review panel.
    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(untilDestroyed(this))
      .subscribe(md => {
        this.operatorCatalog = md;
      });
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    localStorage.setItem("ai-wizard-width", String(this.width));
    localStorage.setItem("ai-wizard-height", String(this.height));
    localStorage.setItem("ai-wizard-docked", String(this.width === 0));
  }

  // ---------- Panel chrome ----------

  openOrClosePanel(): void {
    if (this.width === 0) {
      this.width = AiWizardPanelComponent.MIN_W;
      this.isDocked = false;
    } else {
      this.width = 0;
      this.isDocked = true;
    }
  }

  onResize({ width, height }: NzResizeEvent): void {
    cancelAnimationFrame(this.resizeRaf);
    this.resizeRaf = requestAnimationFrame(() => {
      this.width = width!;
      this.height = height!;
    });
  }

  // ---------- Step navigation ----------

  nextStep(): void {
    if (this.state.step < 4) this.state.step++;
  }
  prevStep(): void {
    if (this.state.step > 1) this.state.step--;
  }

  canProceed(): boolean {
    if (this.state.step === 1) {
      if (!this.state.analysisGoal) return false;
      if (this.state.analysisGoal === "Custom") return !!this.state.customAnalysisGoal?.trim();
      return true;
    }
    if (this.state.step === 2) {
      if (!this.state.dataSource) return false;
      if (this.state.dataSource === "dkNET Dataset") return !!this.state.dknetDataset;
      if (this.state.dataSource === "Existing Dataset") return !!this.state.existingDatasetPath;
      return true;
    }
    if (this.state.step === 3) {
      if (!this.state.framework) return false;
      return !!this.state.frameworkPrompt?.trim();
    }
    return true;
  }

  // ---------- Step 1 actions ----------

  selectGoal(goal: AnalysisGoal): void {
    this.state.analysisGoal = goal;
  }
  onCustomGoalChange(text: string): void {
    this.state.customAnalysisGoal = text;
  }

  // ---------- Step 2 actions ----------

  selectDataSource(source: DataSource): void {
    this.state.dataSource = source;
  }

  /** Open Texera's existing dataset-file picker modal. Returns a path like
   *  "/<owner>/<dataset>/v1/<file>" that CSVFileScan can read on the backend.
   *  We also fetch the file bytes and run PapaParse so the LLM gets a real
   *  data profile (design-doc §4.2 schema-aware generation). */
  openExistingDatasetPicker(): void {
    const modal = this.modalService.create({
      nzContent: DatasetSelectionModalComponent,
      nzFooter: null,
      nzData: {
        fileMode: true,
        selectedPath: this.state.existingDatasetPath ?? "",
      },
      nzBodyStyle: {
        resize: "both",
        overflow: "auto",
        minHeight: "300px",
        minWidth: "600px",
        maxWidth: "90vw",
        maxHeight: "80vh",
      },
      nzWidth: "fit-content",
    });
    modal.afterClose.pipe(untilDestroyed(this)).subscribe((selectedPath: string | undefined) => {
      if (!selectedPath) return;
      this.state.existingDatasetPath = selectedPath;
      this.state.dataProfile = undefined;
      void this.profileExistingDataset(selectedPath);
    });
  }

  private async profileExistingDataset(path: string): Promise<void> {
    try {
      const blob = await firstValueFrom(this.datasetService.retrieveDatasetVersionSingleFile(path, true));
      const text = await blob.text();
      const profile = this.profiler.profileCsvText(text);
      if (profile) {
        this.state.dataProfile = profile;
      } else {
        this.notificationService.warning(
          `Picked dataset file but couldn't parse it as CSV. The workflow will still generate but without a Data Profile.`
        );
      }
    } catch (err) {
      console.warn("Failed to fetch/profile existing dataset:", err);
      this.notificationService.warning(
        "Couldn't fetch the dataset file to profile it. Workflow can still be generated."
      );
    }
  }

  selectDknetDataset(ds: DknetDataset): void {
    this.state.dknetDataset = ds;
    if (ds.profile) {
      this.state.dataProfile = ds.profile;
    }
  }

  // ---------- Step 3 actions ----------

  selectFramework(framework: ScientificFramework): void {
    this.state.framework = framework;
    this.state.frameworkPrompt = FRAMEWORKS[framework].prompt;
  }
  onFrameworkPromptChange(text: string): void {
    this.state.frameworkPrompt = text;
  }
  resetFrameworkTemplate(): void {
    if (this.state.framework) {
      this.state.frameworkPrompt = FRAMEWORKS[this.state.framework].prompt;
    }
  }

  // ---------- Step 4 actions ----------

  toggleGuardrail(id: string): void {
    this.state.guardrails = this.state.guardrails.map(g => (g.id === id ? { ...g, enabled: !g.enabled } : g));
  }

  // ---------- Generate / Apply / NL-edit ----------

  async onGenerate(): Promise<void> {
    if (!this.canGenerate()) return;
    this.isGenerating = true;
    this.generationError = null;
    this.attempts = [];
    try {
      const result = await this.generator.generate(this.state, this.state.model);
      this.applyGeneratedResult(result);
    } catch (err: any) {
      this.attempts = err?.attempts ?? this.attempts;
      this.generationError = err?.message ?? "Failed to generate workflow.";
    } finally {
      this.isGenerating = false;
    }
  }

  onModelChange(modelId: string): void {
    this.state.model = modelId;
    localStorage.setItem("ai-wizard-model", modelId);
  }

  canGenerate(): boolean {
    if (this.isGenerating) return false;
    if (!this.state.analysisGoal) return false;
    if (this.state.analysisGoal === "Custom" && !this.state.customAnalysisGoal?.trim()) return false;
    if (!this.state.dataSource) return false;
    return true;
  }

  applyToCanvas(): void {
    if (!this.generatedWorkflow) return;
    if (!this.reviewApproved) {
      this.notificationService.warning("Please review each operator and click 'Approve all' before Apply.");
      return;
    }
    const currentMeta = this.workflowActionService.getWorkflowMetadata();
    const wf = { ...currentMeta, content: this.generatedWorkflow } as any;
    this.workflowActionService.reloadWorkflow(wf, false, false);
    this.notificationService.success("Workflow applied to canvas.");
  }

  approveAndApply(): void {
    this.reviewApproved = true;
    this.applyToCanvas();
  }

  /**
   * Edit an auto-filled property value in place. Tries to parse JSON first so
   * users can edit nested objects/arrays in the same text field; falls back to
   * storing the raw string for simple string properties.
   */
  updateOperatorProperty(opIndex: number, key: string, newValueRaw: string): void {
    if (!this.generatedWorkflow) return;
    const ops = [...this.generatedWorkflow.operators];
    const op = ops[opIndex];
    if (!op) return;

    let parsed: any = newValueRaw;
    const trimmed = newValueRaw.trim();
    if (
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]")) ||
      /^(true|false|null|-?\d+(\.\d+)?)$/.test(trimmed)
    ) {
      try {
        parsed = JSON.parse(trimmed);
      } catch {
        parsed = newValueRaw; // keep raw if invalid JSON; user can fix
      }
    }

    const updated: OperatorPredicate = {
      ...op,
      operatorProperties: { ...op.operatorProperties, [key]: parsed },
    };
    ops[opIndex] = updated;
    this.generatedWorkflow = { ...this.generatedWorkflow, operators: ops };
    // Manual edit re-arms the review gate so the user must explicitly approve again.
    this.reviewApproved = false;
  }

  /** Look up operator schema (required[] and full properties) from the live catalog. */
  schemaForOperator(operatorType: string): OperatorSchema | undefined {
    return this.operatorCatalog?.operators.find(s => s.operatorType === operatorType);
  }

  /** Properties that the operator's jsonSchema declares as required. */
  requiredKeysFor(operatorType: string): string[] {
    const schema = this.schemaForOperator(operatorType);
    const req = (schema?.jsonSchema as any)?.required;
    return Array.isArray(req) ? req : [];
  }

  /** Helper used by template *ngFor over an operator's properties. */
  propertyEntries(op: OperatorPredicate): { key: string; value: any; required: boolean }[] {
    const required = new Set(this.requiredKeysFor(op.operatorType));
    const props = op.operatorProperties ?? {};
    // Show required keys first (whether filled or missing), then the rest.
    const declaredRequired = Array.from(required);
    const optionalKeys = Object.keys(props).filter(k => !required.has(k));
    const all = [...declaredRequired, ...optionalKeys];
    return all.map(k => ({
      key: k,
      value: props[k],
      required: required.has(k),
    }));
  }

  /** Deep-check: list each operator's missing required paths (incl. nested). */
  missingRequiredPaths(op: OperatorPredicate): string[] {
    const schema = this.schemaForOperator(op.operatorType);
    if (!schema) return [];
    return findMissingRequiredPaths(op.operatorProperties ?? {}, schema.jsonSchema, "");
  }

  /** True if any operator has any unset required property (top or nested). */
  hasMissingRequired(): boolean {
    if (!this.generatedWorkflow) return false;
    return this.generatedWorkflow.operators.some(op => this.missingRequiredPaths(op).length > 0);
  }

  /** trackBy for the property *ngFor — keeps the same input DOM node across
   *  change detection so the user's caret position / focus isn't dropped on every
   *  keystroke. Without this, the input flashes and the value field reads as
   *  uneditable. */
  trackPropertyKey = (_: number, p: { key: string }) => p.key;

  /** trackBy for the operator *ngFor — same reasoning, keeps each operator
   *  card's DOM (and its open/closed <details> state) stable. */
  trackOperatorId = (_: number, op: OperatorPredicate) => op.operatorID;

  formatPropValue(v: any): string {
    if (v === undefined || v === null) return "(unset)";
    if (typeof v === "string") return v;
    try {
      return JSON.stringify(v);
    } catch {
      return String(v);
    }
  }

  downloadJson(): void {
    if (!this.generatedWorkflow) return;
    const payload = { ...this.generatedWorkflow, whyExplanations: this.whyExplanations };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `texera-workflow-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  async onNlEdit(): Promise<void> {
    if (!this.generatedWorkflow || !this.nlInstruction.trim()) return;
    this.isModifying = true;
    this.modificationError = null;
    try {
      const result = await this.generator.modify(
        this.generatedWorkflow,
        this.whyExplanations,
        this.nlInstruction,
        this.state.dataProfile,
        this.state.model
      );
      this.applyGeneratedResult(result);
      this.editHistory = [...this.editHistory, this.nlInstruction];
      this.nlInstruction = "";
    } catch (err: any) {
      this.modificationError = err?.message ?? "Failed to modify workflow.";
    } finally {
      this.isModifying = false;
    }
  }

  private applyGeneratedResult(result: GeneratedWorkflow): void {
    this.generatedWorkflow = result.workflow;
    this.whyExplanations = result.whyExplanations;
    this.attempts = result.attempts;
    // New generation invalidates any prior review approval.
    this.reviewApproved = false;
  }

  guardrailsSummary(): string {
    return getGuardrailsPrompt(this.state.guardrails).slice(0, 200);
  }

  // Helper for template (record entries).
  operatorIdsWithWhy(): { id: string; explanation: string }[] {
    return Object.entries(this.whyExplanations).map(([id, explanation]) => ({ id, explanation }));
  }
}
