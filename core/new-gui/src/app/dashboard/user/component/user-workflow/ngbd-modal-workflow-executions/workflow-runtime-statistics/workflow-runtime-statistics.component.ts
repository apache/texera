import { AfterContentInit, Component, Input, OnInit } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { WorkflowRuntimeStatistics } from "src/app/dashboard/user/type/workflow-runtime-statistics";
import * as Chart from "chart.js";

@UntilDestroy()
@Component({
  selector: "texera-workflow-runtime-statistics",
  templateUrl: "./workflow-runtime-statistics.component.html",
  styleUrls: ["./workflow-runtime-statistics.component.scss"],
})
export class WorkflowRuntimeStatisticsComponent implements OnInit, AfterContentInit {
  @Input()
  workflowRuntimeStatistics?: WorkflowRuntimeStatistics[];

  constructor() {}

  ngOnInit(): void {}

  ngAfterContentInit(): void {
    if (this.workflowRuntimeStatistics === undefined) {
      return;
    }
    // Get the canvas context for chart 1 and chart 2
    const ctx1 = (document.getElementById("chart1") as HTMLCanvasElement).getContext("2d");

    if (ctx1 === null) {
      return;
    }

    // Group the workflowRuntimeStatistics by operatorId
    const groupedStats = this.workflowRuntimeStatistics.reduce(
      (acc: Record<string, WorkflowRuntimeStatistics[]>, stat) => {
        acc[stat.operatorId] = acc[stat.operatorId] || [];
        acc[stat.operatorId].push(stat);
        return acc;
      },
      {}
    );

    // Create an array of datasets for chart 1 (inputTupleCount)
    const datasets1 = Object.keys(groupedStats).map((operatorId, index) => {
      return {
        label: operatorId,
        data: groupedStats[operatorId].map(stat => stat.inputTupleCount),
        fill: false,
      };
    });

    // Create an array of labels for the x-axis (time)
    const labels = groupedStats[Object.keys(groupedStats)[0]].map((stat, index) => index * 0.5);

    // Create the chart 1 object
    const chart1 = new Chart.Chart(ctx1, {
      type: "line",
      data: {
        labels: labels,
        datasets: datasets1,
      },
    });
  }
}
