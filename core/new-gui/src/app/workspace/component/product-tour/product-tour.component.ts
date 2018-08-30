import { Component, OnInit } from '@angular/core';
import { TourService, IStepOption } from 'ngx-tour-ng-bootstrap';

@Component({
  selector: 'texera-product-tour',
  templateUrl: './product-tour.component.html',
  styleUrls: ['./product-tour.component.scss']
})
export class ProductTourComponent implements OnInit {

  public loaded = false;

  constructor(public tourService: TourService) {


    this.tourService.initialize([{
      anchorId: 'texera-navigation-grid-container',
      content: `
      <center>
        <h3>Welcome to Texera!</h3>
      </center>
      <br>
      <p>
      Texera is a system to support cloud-based text analytics using declarative and GUI-based workflows.
      </p>
      <br>
      <center>
      <img src="../../../assets/Tutor_Intro_Sample.png" height="400" width="800" alt="intro img">
      </center>
      <br><br>
      `,
      placement: 'bottom',
      title: 'Welcome',
      preventScrolling: true
    },
    {
      anchorId: 'texera-operator-panel',
      content: '<p>This is the operator panel which contains all the operators we need. </p><p>Now we want to form a twitter text analysis workflow. Open the first section named <b>Source</b>.</p><center><img src="../../../assets/Tutor_OpenSection_Sample.gif"></center><br><br>',
      placement: 'right',
      title: 'Operator Panel',
      preventScrolling: true
    },
    {
      anchorId: 'Source: Scan',
      content: '<p>Drag <b>Source: Scan</b> and drop to workflow panel. </p><p>Source: Scan is a operator that read records from a table one by one.</p><center><img src="../../../assets/Tutor_Intro_Drag_Srouce.gif"></center><br><br>',
      title: 'Select Operator',
      placement: 'right',
      preventScrolling: true
    },
    {
      anchorId: 'texera-workflow-editor-grid-container',
      content: '<p>This is the workflow Panel</p><p>We can form a workflow by connecting two or more operators.</p>',
      title: 'Workflow Panel',
      placement: 'bottom'
    },
    {
      anchorId: 'texera-property-editor-grid-container',
      content: '<p>This is operator editor area which we can set the properties of the operator. </p><p>Now we want to edit the property of Source: Scan Operator by typing <b>twitter_sample</b> which specify the data we want to use.</p><center><img src="../../../assets/Tutor_Property_Sample.gif"></center><br><br>',
      placement: 'left',
      title: 'Property Editor'
    },
    {
      anchorId: 'View Results',
      content: '<p>Now we want to view the results of selected data. Open <b>View Results</b> section.</p><center><img src="../../../assets/Tutor_OpenResult_Sample.gif"></center><br><br>',
      placement: 'right',
      title: 'Operator Panel'
    },
    {
      anchorId: 'View Results',
      content: '<p>Drag <b>View Results</b> and drop to workflow panel.</p><center><img src="../../../assets/Tutor_Intro_Drag_Result.gif"></center><br><br>',
      placement: 'right',
      title: 'Select Operator',
    },
    {
      anchorId: 'texera-workflow-editor-grid-container',
      content: '<p>Connect those two operators.</p><center><img src="../../../assets/Tutor_JointJS_Sample.gif"></center><br><br>',
      placement: 'bottom',
      title: 'Connecting operators'
    },
    {
      anchorId: 'texera-workspace-navigation-run',
      content: '<p>Click the run button.</p>',
      title: 'Running the workflow',
      placement: 'left'
    },
    {
      anchorId: 'texera-result-view-grid-container',
      content: '<p>You can view the results here.</p>',
      placement: 'top',
      title: 'Viewing the results'
    },
    {
      anchorId: 'texera-navigation-grid-container',
      content: '<center><h3>Congratulations!</h3></center><p>You have finished the basic tutorial. </p><p>There are many other operators you can use to form a workflow.</p><center><img src="../../../assets/Tutor_End_Sample.gif"></center><br><br>',
      placement: 'bottom',
      title: 'Ending of tutorial'
    }]);

  }

  ngOnInit() {
  }

}
