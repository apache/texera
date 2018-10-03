import { Component, OnInit } from '@angular/core';
import { TourService, IStepOption } from 'ngx-tour-ng-bootstrap';

/**
 * ProductTourComponent is the product tour that shows basic product tutorial.
 *
 * Product tour library built with Angular (2+).
 * ngx-tour-ngx-bootstrap is an implementation of the tour ui that uses ngx-bootstrap popovers to display tour steps.
 *
 * The component has a step list in this.tourService.initialize that can add, edit or delete steps.
 * Define anchor points for the tour steps by adding the tourAnchor directive throughout components.
 *
 * <div tourAnchor="some.anchor.id">...</div>
 *
 * Define your tour steps using tourService.initialize(steps).
 *
 * For the full text of the library, go to https://github.com/isaacplmann/ngx-tour
 *
 * The screenshots were done by GIPHY Capture
 *
 *
 * @author Bolin Chen
 */

@Component({
  selector: 'texera-product-tour',
  templateUrl: './product-tour.component.html',
  styleUrls: ['./product-tour.component.scss']
})
export class ProductTourComponent implements OnInit {

  private steps: IStepOption[] = [{
    anchorId: 'texera-navigation-grid-container',
    content: `
    <div class="intro">
    <center>
      <h3>Welcome to Texera!</h3>
    </center>
    <br>
    <p>
    Texera is a system to support cloud-based text analytics using declarative and GUI-based workflows. Use '« Prev' and
     'Next »' or left and right arrow keys to navigate through the tutorial.
    </p>
    <br>
    <center>
    <img src="../../../assets/Tutor_Intro_Sample.jpeg" alt="intro img">
    </center>
    <br><br>
    </div>
    `,
    placement: 'bottom',
    title: 'Welcome',
    preventScrolling: true
  },
  {
    anchorId: 'texera-operator-panel',
    content: `
    <p>This is the operator panel which contains all the operators we need. </p>
    <p>Now we want to form a twitter text analysis workflow. Open the first section named <b>Source</b>.</p>
    <center><img src="../../../assets/Tutor_OpenSection_Sample.gif"></center>
    <br><br>
    `,
    placement: 'right',
    title: 'Operator Panel',
  },
  {
    anchorId: 'texera-operator-panel',
    content: `
    <p>Drag <b>Source: Scan</b> and drop to workflow panel. </p>
    <p>Source: Scan is a operator that read records from a table one by one.</p>
    <center><img src="../../../assets/Tutor_Intro_Drag_Srouce.gif"></center>
    <br><br>
    `,
    title: 'Select Operator',
    placement: 'right',
    preventScrolling: true
  },
  {
    anchorId: 'texera-property-editor-grid-container',
    content: `
    <p>This is operator editor area which we can set the properties of the operator. </p>
    <p>Now we want to edit the property of Source: Scan Operator by typing <b>twitter_sample</b> which specify the data we want to use.</p>
    <center><img src="../../../assets/Tutor_Property_Sample.gif"></center>
    <br><br>
    `,
    placement: 'left',
    title: 'Property Editor',
    preventScrolling: true
  },
  {
    anchorId: 'View Results',
    content: `<p>Now we want to view the results of selected data. Open <b>View Results</b> section.</p>
    <center><img src="../../../assets/Tutor_OpenResult_Sample.gif"></center>
    <br><br>
    `,
    placement: 'right',
    title: 'Operator Panel',
    preventScrolling: true
  },
  {
    anchorId: 'texera-operator-label-ViewResults',
    content: `
    <p>Drag <b>View Results</b> and drop to workflow panel.</p>
    <center><img src="../../../assets/Tutor_Intro_Drag_Result.gif"></center>
    <br><br>
    `,
    placement: 'right',
    title: 'Select Operator',
    preventScrolling: true
  },
  {
    anchorId: 'texera-property-editor-grid-container',
    content: `
    <p>Connect those two operators.</p><center>
    <img src="../../../assets/Tutor_JointJS_Sample.gif"></center>
    <br><br>
    `,
    placement: 'left',
    title: 'Connecting operators',
    preventScrolling: true
  },
  {
    anchorId: 'texera-workspace-navigation-run',
    content: `
    <p>Click the run button.</p>
    `,
    title: 'Running the workflow',
    placement: 'bottom',
    preventScrolling: true
  },
  {
    anchorId: 'texera-result-view-grid-container',
    content: `
    <p>You can view the results here.</p>
    `,
    placement: 'top',
    title: 'Viewing the results',
    preventScrolling: true
  },
  {
    anchorId: 'texera-navigation-grid-container',
    content: `
    <center><h3>Congratulations!</h3></center>
    <p>You have finished the basic tutorial. </p>
    <p>There are many other operators you can use to form a workflow.</p>
    <center><img src="../../../assets/Tutor_End_Sample.gif"></center>
    <br><br>
    `,
    placement: 'bottom',
    title: 'Ending of tutorial',
    preventScrolling: true
  }];

  constructor(public tourService: TourService) {

    this.tourService.initialize(this.steps);

  }

  ngOnInit() {
  }

}
