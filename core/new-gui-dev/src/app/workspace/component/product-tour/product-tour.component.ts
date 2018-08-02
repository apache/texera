import { Component, OnInit } from '@angular/core';
import { NgxBootstrapProductTourService } from 'ngx-bootstrap-product-tour';
@Component({
  selector: 'texera-product-tour',
  templateUrl: './product-tour.component.html',
  styleUrls: ['./product-tour.component.scss']
})
export class ProductTourComponent implements OnInit {

  public loaded = false;

  constructor(public tourService: NgxBootstrapProductTourService) { 
    this.loaded = false;
    this.tourService.end$.subscribe(() => {
      location.reload();
    });
    this.tourService.events$.subscribe(console.log);
    this.tourService.initialize([{
      anchorId: 'start.tour',
      content: '<center><h3>Welcome to Texera!</h3></center><br><p>Texera is a system to support cloud-based text analytics using declarative and GUI-based workflows.</p><br><img src="../../../../assets/Tutor_Intro_Sample.png" height="400" width="800"><br><br>',
      placement: 'right',
      title: 'Welcome',
      orphan: true,
      backdrop: true
    },
    {
      anchorId: 'texera-operator-view-grid-container',
      content: '<p>This is the operator panel which contains all the operators we need. Now we want to form a twitter text analysis workflow. Open the first section named <b>Source</b>.</p>',
      placement: 'right',
      title: 'Operator Panel',
      backdrop: true
    },
    {
      anchorId: 'texera-operator-label-ScanSource',
      content: '<p>Drag <b>Source: Scan</b> and drop to workflow panel. Source: Scan is a operator that read records from a table one by one.</p><img src="../../../../assets/Tutor_Intro_Drag_Srouce.gif"><br><br>',
      title: 'Select Operator',
      placement: 'right',
      
    },
    {
      anchorId: 'texera-workflow-editor-grid-container',
      content: '<p>This is the workflow Panel</p><p>We can form a workflow by connecting two or more operators.</p>',
      title: 'Workflow Panel',
      placement: 'bottom',
      backdrop: true
    },
    {
      anchorId: 'texera-property-editor-grid-container',
      content: '<p>This is operator editor area which we can set the properties of the operator. Now we want to edit the property of Source: Scan Operator by typing <b>twitter_sample</b> which specify the data we want to use.</p><img src="../../../../assets/Tutor_Property_Sample.gif"><br><br>',
      placement: 'left',
      title: 'Property Editor',
      backdrop: true
    },
    {
      anchorId: 'View Results',
      content: '<p>Now we want to view the results of selected data. Open <b>View Results</b> secetion.</p>',
      placement: 'right',
      title: 'Operator Panel',
      backdrop: true
    },
    {
      anchorId: 'texera-operator-label-ViewResults',
      content: '<p>Drag <b>View Results</b> and drop to workflow panel.</p><img src="../../../../assets/Tutor_Intro_Drag_Result.gif"><br><br>',
      placement: 'right',
      title: 'Select Operator',
    },
    {
      anchorId: 'texera-workflow-editor-grid-container',
      content: '<p>Connect those two operators.</p><img src="../../../../assets/Tutor_JointJS_Sample.gif"><br><br>',
      placement: 'bottom',
      title: 'Connecting operators',
      backdrop: true
    },
    {
      anchorId: 'texera-workspace-navigation-run',
      content: '<p>Click the run button.</p>',
      title: 'Running the workflow',
      placement: 'left',
      backdrop: true
    },
    {
      anchorId: 'texera-result-view-grid-container',
      content: '<p>You can view the results here.</p>',
      placement: 'top',
      title: 'Viewing the results',
      backdrop: true
    },
    {
      anchorId: 'start.tour',
      content: '<center><h3>Congratulation!</h3></center><p>You have finished the basic tutorial. There are many other operators you can use to form a convenient and efficient workflow.</p><center><img src="../../../../assets/Tutor_End_Sample.gif"></center><br><br>',
      placement: 'right',
      title: 'Ending of tutorial',
      orphan: true,
      backdrop: true
    }]);
  } 

  ngOnInit() {
  }

}
