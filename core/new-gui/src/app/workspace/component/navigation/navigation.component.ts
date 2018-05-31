import { Component, OnInit } from '@angular/core';
import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';


@Component({
  selector: 'texera-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {

  // variable binded with HTML to decide if the running spinner should show
  public showSpinner = false;

  constructor(private executeWorkflowService: ExecuteWorkflowService) {
    // hide the spinner after the execution is finished
    executeWorkflowService.getExecuteEndedStream().subscribe(
      value => this.showSpinner = false,
      error => this.showSpinner = false
    );
  }

  ngOnInit() {
  }

  private onClickRun(): void {
    // show the spinner after the "Run" button is clicked
    this.showSpinner = true;
    this.executeWorkflowService.executeWorkflow();
  }

}
