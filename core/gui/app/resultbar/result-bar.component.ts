import { Component,ViewChild, OnInit } from '@angular/core';
import { CurrentDataService } from '../services/current-data-service';
import { ModalComponent } from 'ng2-bs3-modal/ng2-bs3-modal';

declare var jQuery: any;
declare var Backbone: any;
declare var PrettyJSON: any;
var previousOpenHeight: number = 300;

@Component({
    moduleId: module.id,
    selector: 'result-container',
    templateUrl: './result-bar.component.html',
    styleUrls: ['../style.css'],
})
export class ResultBarComponent {
  // result = entire json result from backend
  result: any[] = [];
  // attribute = all the keys to access the result
  attribute: string[] = [];
  previousResultHandleTop: number = -5;
  checkErrorOrDetail: number = 1;
  ResultDisplayLimit: number = 20;
  CurrentDisplayIndex: number;

  @ViewChild('ResultModal')
  modal: ModalComponent;

  ModalOpen() {
    this.modal.open();
  }
  ModalClose() {
    this.modal.close();
  }

  PreviousModal() {
    jQuery("#ModalNextButton").prop('disabled',false);
    var previousResult = this.result[this.CurrentDisplayIndex - 1];
    var node = new PrettyJSON.view.Node({
      el: jQuery("#ResultElem"),
      data: previousResult
    });
    --this.CurrentDisplayIndex;
    if (this.CurrentDisplayIndex === 0) {
      jQuery("#ModalPreviousButton").prop('disabled',true);
    }
  }

  NextModal() {
    jQuery("#ModalPreviousButton").prop('disabled',false);
    var nextResult = this.result[this.CurrentDisplayIndex + 1];
    var node = new PrettyJSON.view.Node({
      el: jQuery("#ResultElem"),
      data: nextResult
    });
    ++this.CurrentDisplayIndex;
    if (this.CurrentDisplayIndex === this.result.length - 1) {
      jQuery("#ModalNextButton").prop('disabled',true);
    }
  }
  constructor (private currentDataService: CurrentDataService){
    currentDataService.checkPressed$.subscribe(
      data => {
        // stop the loading animation of the run button
        jQuery('.navigation-btn').button('reset');
        this.attribute = [];
        this.result = [];
        this.checkErrorOrDetail = 1;
        // check if the result is valid
        if (data.code == -1) {
          //Json Mapping Exception would not be handled for auto plan
        }else if (data.code === 0) {
          console.log(data.result);
          var ResultDisplay = (data.result.length < 20) ? data.result.length : this.ResultDisplayLimit;
          for (var i = 0; i < ResultDisplay; ++i) {
            this.result.push(data.result[i]);
          }
          for (var each in this.result[0]){
            if (each !== "_id"){
              this.attribute.push(each);
            }
          }
          // open the result bar automatically
          this.openResultBar();
          this.changeSuccessStyle();
        } else {
          // pop the modal when not valid
            this.checkErrorOrDetail = 0;
            var node = new PrettyJSON.view.Node({
              el: jQuery("#ResultElem"),
              data: {"message": data.message}
            });
            this.ModalOpen();
            this.closeResultBar();
            this.changeErrorStyle();
        }
      },
      err => {
        jQuery('.navigation-btn').button('reset');
        this.checkErrorOrDetail = 0;
        var node = new PrettyJSON.view.Node({
          el: jQuery("#ResultElem"),
          data: {"message": "Network Error"},
        });
        this.ModalOpen();
        this.closeResultBar();
        this.changeErrorStyle();
      }
    );
  }

  changeSuccessStyle(){
    jQuery("#result-bar-title").css({"background-color" : "#4d79ff"});
    jQuery("#result-bar-title").hover(
      function() {jQuery("#result-bar-title").css({"background-color" : "#1a53ff"})},
      function() {jQuery("#result-bar-title").css({"background-color" : "#4d79ff"})});
    jQuery( "#result-bar-title" ).html( "<b>Results</b>");
  }

  changeErrorStyle(){
    jQuery("#result-bar-title").css({"background-color" : "#ff3333"});
    jQuery("#result-bar-title").hover(
      function() {jQuery("#result-bar-title").css({"background-color" : "#ff0000"})},
      function() {jQuery("#result-bar-title").css({"background-color" : "#ff3333"})});
    jQuery("#result-bar-title").html( "<b>Error</b>");
  }

  shortenString(longText: string){
    longText = longText.toString();
    if (longText.length < 25) {
      return longText;
    } else {
      return longText.substring(0,25) + " ...";
    }
  }
  checkIfObject(eachAttribute: string){
    return typeof eachAttribute === "object";
  }
  resultBarClicked(){
    // check if the result bar is opened or closed
    var currentResultBarStatus = jQuery('#result-table-bar').css('display');
    if (currentResultBarStatus === "none"){
      this.openResultBar()
    } else {
      this.closeResultBar()
    }
  }

  openResultBar(){
    jQuery("#result-table-bar").css({
      "display":"block",
      "height" : previousOpenHeight + "px",
    });
    var new_container_height = previousOpenHeight + 40;
    jQuery('#ngrip').css({"top":"-5px"});
    jQuery("#flow-chart-container").css({"height":"calc(100% - " + new_container_height + "px)"});
    this.redrawResultBar();
  }

  closeResultBar(){
    jQuery("#result-table-bar").css({
      "display":"none",
      "height" : "0px",
    });
    jQuery('#ngrip').css({"top":"-5px"});
    jQuery("#flow-chart-container").css({"height":"calc(100% - 40px)"});
    this.redrawResultBar();
  }

  redrawResultBar(){
    this.previousResultHandleTop = -parseInt(jQuery('#result-table-bar').css('height'), 10) - 5;
    jQuery("#ngrip").draggable( "destroy" );
    this.initializeResizing(this.previousResultHandleTop);
  }


  displayRowDetail(singleResult: any){
    // restore default
    jQuery("#ModalPreviousButton").prop('disabled',false);
    jQuery("#ModalNextButton").prop('disabled',false);
    var count = 0;
    for (var each of this.result){
      if (each === singleResult){
        break;
      }
      ++count;
    }
    this.CurrentDisplayIndex = count;
    if (this.CurrentDisplayIndex === 0) {
      jQuery("#ModalPreviousButton").prop('disabled',true);
    } else if (this.CurrentDisplayIndex === this.result.length - 1) {
      jQuery("#ModalNextButton").prop('disabled',true);
    }
    var node = new PrettyJSON.view.Node({
      el: jQuery("#ResultElem"),
      data: singleResult
    });
    this.ModalOpen();
  }

  // initialized the default draggable / resizable result bar
  initializing(){
    this.initializeResizing(this.previousResultHandleTop);
  }

  initializeResizing(previousHeight: number){
    jQuery("#ngrip").draggable({
      axis:"y",
      containment: "window",
      drag: function( event, ui ) {
        // calculate the position
        var endPosition = previousHeight + 5 + ui.position.top;
        // if endPosition exceeds the maximum
        if (endPosition < -305){
          ui.position.top = -305;
          jQuery("#result-table-bar").css({
            "display":"block",
            "height" : "300px",
          });
          jQuery("#flow-chart-container").css({"height":"calc(100% - 340px)"});
        } else if (endPosition > -5){
          // if endPosition is lower than the minimum
          ui.position.top = -5;
          jQuery("#result-table-bar").css({
            "display":"none",
            "height" : "0px",
          });
          jQuery("#flow-chart-container").css({"height":"calc(100% - 40px)"});
        } else {
          var newResultBarHeight = -endPosition - 5; // include the drag button
          var newFlowChartContainerHeight = newResultBarHeight + 40; // include the drag button and the title bar

          // redraw 2 fields to resize
          jQuery("#flow-chart-container").css({"height":"calc(100% - " + newFlowChartContainerHeight + "px)"});
          jQuery("#result-table-bar").css({
            "display":"block",
            "height" : newResultBarHeight + "px",
          });
        }
      },

      stop: function( event, ui ) {
        // get the current result bar height
        var newResultBarHeight = parseInt(jQuery('#result-table-bar').css('height'), 10);
        var ResultBarDisplay = jQuery('#result-table-bar').css('display');
        // if at minimum
        if (newResultBarHeight === 0 || ResultBarDisplay === "none"){
          previousHeight = -5;
          previousOpenHeight = 300; //restore default
        } else if (newResultBarHeight === 300){
          previousHeight = -305;
          previousOpenHeight = newResultBarHeight;
        } else {
          // previous height is used for calculating the movement of the result bar and flowchart
          previousHeight = previousHeight + 5 + parseInt(jQuery('#ngrip').css('top'), 10);
          previousOpenHeight = newResultBarHeight;
        }
        // make sure drag button is directly above the result bar
        jQuery('#ngrip').css({"top":"-5px"});

      }
    });

  }

}
