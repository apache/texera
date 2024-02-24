import { AfterContentInit, Component, Input } from "@angular/core";
import { DomSanitizer } from "@angular/platform-browser";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { auditTime } from "rxjs/operators";
import { untilDestroyed, UntilDestroy } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-visualization-panel-content",
  templateUrl: "./visualization-frame-content.component.html",
  styleUrls: ["./visualization-frame-content.component.scss"],
})
export class VisualizationFrameContentComponent implements AfterContentInit {
  // progressive visualization update and redraw interval in milliseconds
  public static readonly UPDATE_INTERVAL_MS = 2000;

  htmlData: any = "";

  @Input()
  operatorId?: string;
  data: ReadonlyArray<object> = [];
  columns: string[] = [];

  constructor(
    private workflowResultService: WorkflowResultService,
    private sanitizer: DomSanitizer
  ) {}

  ngAfterContentInit() {
    // attempt to draw chart immediately
    this.drawChart();

    // setup an event lister that re-draws the chart content every (n) milliseconds
    // auditTime makes sure the first re-draw happens after (n) milliseconds has elapsed
    this.workflowResultService
      .getResultUpdateStream()
      .pipe(auditTime(VisualizationFrameContentComponent.UPDATE_INTERVAL_MS))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.drawChart();
      });
  }
  drawChart() {
    if (!this.operatorId) {
      return;
    }
    const operatorResultService = this.workflowResultService.getResultService(this.operatorId);
    if (!operatorResultService) {
      return;
    }
    this.data = operatorResultService.getCurrentResultSnapshot() ?? [];
    if (!this.data) {
      return;
    }
    if (this.data?.length < 1) {
      return;
    }
    this.htmlData = this.sanitizer.bypassSecurityTrustHtml(Object(this.data[0])["html-content"]); // this line bypasses angular security
  }
}
