import { AfterContentInit, Component, Input, OnInit } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { WorkflowRuntimeStatistics } from "src/app/dashboard/user/type/workflow-runtime-statistics";
import Chart from "chart.js/auto";

@UntilDestroy()
@Component({
  selector: "texera-workflow-runtime-statistics",
  templateUrl: "./workflow-runtime-statistics.component.html",
  styleUrls: ["./workflow-runtime-statistics.component.scss"],
})
export class WorkflowRuntimeStatisticsComponent implements OnInit {
  @Input()
  workflowRuntimeStatistics?: WorkflowRuntimeStatistics[];

  constructor() {}

  ngOnInit(): void {
    if (this.workflowRuntimeStatistics === undefined) {
      return;
    }

    const ctx1 = this.getCanvasContext("chart1");
    if (ctx1 === null) {
      return;
    }

    const groupedStats = this.groupStatsByOperatorId(this.workflowRuntimeStatistics);
    const datasets1 = this.createDatasets(groupedStats);
    const labels = this.createLabels(groupedStats);

    this.createChart(ctx1, labels, datasets1);
  }

  getCanvasContext(elementId: string): CanvasRenderingContext2D | null {
    return (document.getElementById(elementId) as HTMLCanvasElement).getContext("2d");
  }

  groupStatsByOperatorId(stats: WorkflowRuntimeStatistics[]): Record<string, WorkflowRuntimeStatistics[]> {
    return stats.reduce((acc: Record<string, WorkflowRuntimeStatistics[]>, stat) => {
      acc[stat.operatorId] = acc[stat.operatorId] || [];
      if (acc[stat.operatorId].length > 0) {
        stat.inputTupleCount =
          stat.inputTupleCount + acc[stat.operatorId][acc[stat.operatorId].length - 1].inputTupleCount;
      }
      acc[stat.operatorId].push(stat);
      return acc;
    }, {});
  }

  createDatasets(groupedStats: Record<string, WorkflowRuntimeStatistics[]>): any[] {
    return Object.keys(groupedStats).map((operatorId, index) => {
      return {
        label: operatorId,
        data: groupedStats[operatorId].map(stat => stat.inputTupleCount),
        fill: false,
      };
    });
  }

  createLabels(groupedStats: Record<string, WorkflowRuntimeStatistics[]>): number[] {
    return groupedStats[Object.keys(groupedStats)[0]].map((stat, index) => index * 0.5);
  }

  createChart(ctx: CanvasRenderingContext2D, labels: number[], datasets: any[]): void {
    new Chart(ctx, {
      type: "line",
      data: {
        labels: labels,
        datasets: datasets,
      },
      options: {
        responsive: true,
        plugins: {
          title: {
            display: true,
            text: "Input Tuple Count",
          },
        },
        scales: {
          y: {
            title: {
              display: true,
              text: "Tuple Count",
            },
          },
          x: {
            title: {
              display: true,
              text: "Time (s)",
            },
          },
        },
      },
    });
  }
}
