import { AfterContentInit, Component, Input } from "@angular/core";
import { DomSanitizer } from "@angular/platform-browser";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { auditTime, filter } from "rxjs/operators";
import { untilDestroyed, UntilDestroy } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-visualization-panel-content",
  templateUrl: "./visualization-frame-content.component.html",
  styleUrls: ["./visualization-frame-content.component.scss"],
})
export class VisualizationFrameContentComponent implements AfterContentInit {
  // operatorId: string = inject(NZ_MODAL_DATA).operatorId;
  @Input() operatorId?: string;
  // progressive visualization update and redraw interval in milliseconds
  public static readonly UPDATE_INTERVAL_MS = 2000;
  htmlData: any = "";

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
      .pipe(filter(rec => this.operatorId !== undefined && rec[this.operatorId] !== undefined))
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
    const data = operatorResultService.getCurrentResultSnapshot();
    if (!data) {
      return;
    }

    const parser = new DOMParser();
    const doc = parser.parseFromString(Object(data[0])["html-content"], "text/html");

    doc.documentElement.style.height = "100%";
    doc.body.style.height = "95%";

    const firstDiv = doc.body.querySelector("div");
    if (firstDiv) firstDiv.style.height = "100%";

    const serializer = new XMLSerializer();
    const newHtmlString = serializer.serializeToString(doc);

    this.htmlData = this.sanitizer.bypassSecurityTrustHtml(newHtmlString); // this line bypasses angular security
  }
}
