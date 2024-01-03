import { Component, Input, OnInit } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { WorkflowRuntimeStatistics } from "src/app/dashboard/user/type/workflow-runtime-statistics";
import * as Plotly from "plotly.js-dist-min";
import { MatTabChangeEvent } from "@angular/material/tabs";

@UntilDestroy()
@Component({
  selector: "texera-workflow-runtime-statistics",
  templateUrl: "./workflow-runtime-statistics.component.html",
  styleUrls: ["./workflow-runtime-statistics.component.scss"],
})
export class WorkflowRuntimeStatisticsComponent {
  @Input()
  workflowRuntimeStatistics?: WorkflowRuntimeStatistics[];

  private tab_index = 0;
  private groupedStats?: Record<string, WorkflowRuntimeStatistics[]>;
  public metrics: string[] = ["Input Tuple Count", "Output Tuple Count"];

  constructor() {}

  onTabChanged(event: MatTabChangeEvent): void {
    this.tab_index = event.index;
    //this.createChart();
  }
}
