import { Component, OnInit, NgModule } from '@angular/core';
import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { TourService } from 'ngx-tour-ng-bootstrap';
import { DragDropService } from './../../service/drag-drop/drag-drop.service';
/**
 * NavigationComponent is the top level navigation bar that shows
 *  the Texera title and workflow execution button
 *
 * This Component will be the only Component capable of executing
 *  the workflow in the WorkflowEditor Component.
 *
 * Clicking the run button on the top-right hand corner will begin
 *  the execution. During execution, the run button will be unavailble
 *  and a spinner will be displayed to show that graph is under execution.
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
@NgModule(
  {
    providers: [DragDropService]
  }
)
export class NavigationComponent implements OnInit {

  // variable binded with HTML to decide if the running spinner should show
  public showSpinner = false;
  private offsetZoom: number = 1;
  constructor(private dragDropService: DragDropService,
    private executeWorkflowService: ExecuteWorkflowService, public tourService: TourService) {
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
  /**
   * send the offset value to the work flow editor panel using drag and drop service.
  */
  public onClickZoomButton(): void {
    this.offsetZoom += 0.01;
    // console.log('send offsetZoom: ', this.offsetZoom);
    this.dragDropService.SetZoomX(this.offsetZoom);
    this.dragDropService.SetZoomY(this.offsetZoom);
    this.dragDropService.handleZoomBus.next(this.offsetZoom);
  }
  public onClickShrinkButton(): void {
    this.offsetZoom -= 0.01;
    this.dragDropService.SetZoomX(this.offsetZoom);
    this.dragDropService.SetZoomY(this.offsetZoom);
    this.dragDropService.handleZoomBus.next(this.offsetZoom);
  }
}
