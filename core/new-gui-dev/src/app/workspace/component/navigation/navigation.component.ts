import { Component, OnInit } from '@angular/core';

import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import { NgxBootstrapProductTourService } from 'ngx-bootstrap-product-tour';


/**
 * NavigationComponenet is the header on the top, it consists of the Texera Label and the Run button.
 *
 * After the Run button is clicked, it notifies the ExecuteWorkflowService to initiate the execution.
 *
 */
@Component({
  selector: 'texera-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {

  // variable binded with HTML to decide if the running spinner should show
  showSpinner = false;

  constructor(private executeWorkflowService: ExecuteWorkflowService, public tourService: NgxBootstrapProductTourService) {
    // hide the spinner after the execution is finished
    executeWorkflowService.executeFinished$.subscribe(
      value => this.showSpinner = false,
      error => this.showSpinner = false
    );
  }

  ngOnInit() {
  }

  onClickRun() {
    // show the spinner after the "Run" button is clicked
    this.showSpinner = true;
    this.executeWorkflowService.executeWorkflow();
  }

}
