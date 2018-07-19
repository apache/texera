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
    this.tourService.initialize([{
      anchorId: 'start.tour',
      content: '<h1>Welcome to Texera!</h1><br><font size="5">Texera is a system to support cloud-based text analytics using declarative and GUI-based workflows.</font><br><br><img src="../../../../assets/Tutor_Intro_Sample.png" height="400" width="800">',
      placement: 'right',
      title: 'Welcome',
      orphan: true,
      backdrop: true
    },
    {
      anchorId: 'texera-operator-view-grid-container',
      content: 'First, We go to operator lists. Now we will open the first section named <b>Source</b>',
      placement: 'right',
      backdrop: true
    },
    {
      anchorId: 'Source',
      content: 'Now open this section',
      placement: 'right',
      backdrop: true
    },
    {
      anchorId: 'texera-operator-label-ScanSource',
      content: 'Drag this and drop to workflow<br><img src="../../../../assets/Tutor_Intro_Drag_Srouce.gif">',
      placement: 'right',
      backdrop: true
    },
    {
      anchorId: 'texera-workflow-editor-grid-container',
      content: '<h1>Here is the workflow Panel</h1><br> now we will edit the operator attribute',
      placement: 'right',
      backdrop: true
    },
    {
      anchorId: 'texera-property-editor-grid-container',
      content: '<h1>Here is the <b>Operator Property Editor</b></h1><b> now try to edit the property of SourceScan Operator, type <b>twitter_sample</b><br><img src="../../../../assets/Tutor_Property_Sample.gif">',
      placement: 'left',
      backdrop: true
    },
    {
      anchorId: 'View Results',
      content: 'Now open this section',
      placement: 'right',
      backdrop: true
    },
    {
      anchorId: 'texera-operator-view-grid-container',
      content: 'Drag this and drop to workflow<br><img src="../../../../assets/Tutor_Intro_Drag_Result.gif">',
      placement: 'right',
      backdrop: true
    },
    {
      anchorId: 'texera-workflow-editor-grid-container',
      content: '<h1>Connect those two operators</h1><br><img src="../../../../assets/Tutor_JointJS_Sample.gif">',
      placement: 'right',
      backdrop: true
    },
    {
      anchorId: 'texera-workspace-navigation-run',
      content: '<h1>Click the run button</h1>',
      placement: 'bottom',
      backdrop: true
    },
    {
      anchorId: 'texera-result-view-grid-container',
      content: '<h1>You can view the result here</h1>',
      placement: 'top',
      backdrop: true
    },
    {
      anchorId: 'start.tour',
      content: '<h1>Congratulation!</h1><br><h2>You have finished the basic tutorial</h2><br><img src="../../../../assets/Tutor_End_Sample.gif">',
      placement: 'right',
      orphan: true,
      backdrop: true
    }]);
  } 

  ngOnInit() {
  }

}
