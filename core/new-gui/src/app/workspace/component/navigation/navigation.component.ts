import { Component, OnInit } from '@angular/core';
import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { UndoRedoService } from './../../service/undo-redo/undo-redo.service';
import { TourService } from 'ngx-tour-ng-bootstrap';

/**
 * NavigationComponent is the top level navigation bar that shows
 *  the Texera title and workflow execution button
 *
 * This Component will be the only Component capable of executing
 *  the workflow in the WorkflowEditor Component.
 *
 * Clicking the run button on the top-right hand corner will begin
 *  the execution. During execution, the run button will be replaced
 *  with a pause/resume button to show that graph is under execution.
 *
 * @author Zuozhi Wang
 * @author Henry Chen
 *
 */
@Component({
  selector: 'texera-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {

  public isWorkflowRunning: boolean = false; // set this to true when the workflow is started
  public isWorkflowPaused: boolean = false; // this will be modified by clicking pause/resume while the workflow is running

  constructor(private executeWorkflowService: ExecuteWorkflowService, public tourService: TourService, public undoRedo: UndoRedoService) {
    // return the run button after the execution is finished, either
    //  when the value is valid or invalid
    executeWorkflowService.getExecuteEndedStream().subscribe(
      () => this.isWorkflowRunning = false,
      () => this.isWorkflowPaused = false
    );
  }

  ngOnInit() {
  }

  /**
   * Executes the current existing workflow on the JointJS paper. It will
   *  also set the `isWorkflowRunning` variable to true to show that the backend
   *  is loading the workflow by displaying the pause/resume button.
   */
  public onClickRun(): void {
    // modifying the `running` and `isPaused` variables will display the pause button
    this.isWorkflowRunning = true;
    this.executeWorkflowService.executeWorkflow();
  }

  /**
   * Pauses/resumes the current existing workflow on the JointJS paper. It will
   * also swap the button between pause and resume whenever you click on it to show whether
   * the backend is currently paused or not.
   */
  public onClickPauseResumeToggle(): void {
    this.isWorkflowPaused = !this.isWorkflowPaused;
  }

}
