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
    // hide the spinner after the execution is finished, either
    //  when the value is valid or invalid
    executeWorkflowService.getExecuteEndedStream().subscribe(
      value => this.showSpinner = false,
      error => this.showSpinner = false
    );
  }

  ngOnInit() {
  }

  /**
   * Executes the current existing workflow on the JointJS paper. It will
   *  also set the `showSpinner` variable to true to show that the backend
   *  is loading the workflow by addding a active spinner next to the
   *  run button.
   */
  public onClickRun(): void {
    // show the spinner after the "Run" button is clicked
    this.showSpinner = true;
    this.executeWorkflowService.executeWorkflow();
  }

}
