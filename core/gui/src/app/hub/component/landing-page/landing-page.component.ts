import { Component, OnInit } from "@angular/core";
import { Observable } from "rxjs";
import { HubWorkflowService } from "../../service/workflow/hub-workflow.service";
import { untilDestroyed } from "@ngneat/until-destroy";
import { Router } from "@angular/router";

@Component({
  selector: "texera-landing-page",
  templateUrl: "./landing-page.component.html",
  styleUrls: ["./landing-page.component.scss"],
})
export class LandingPageComponent implements OnInit {
  public workflowCount: number = 0;

  constructor(
    private hubWorkflowService: HubWorkflowService,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.getWorkflowCount();
  }

  getWorkflowCount(): void {
    this.hubWorkflowService
      .getWorkflowCount()
      // eslint-disable-next-line rxjs-angular/prefer-takeuntil
      .subscribe((count: number) => {
        this.workflowCount = count;
      });
  }

  navigateToSearch(): void {
    this.router.navigate(["/dashboard/hub/workflow/result"]);
  }
}
