import { Component } from '@angular/core';
import { CurrentDataService } from '../services/current-data-service';
import any = jasmine.any;

declare var jQuery: any;

const MIN_SCALE = 0.5;
const MAX_SCALE = 1;
const INCREMENT = 0.1;

@Component({
  moduleId: module.id,
  selector: 'flowchart-container',
  template: `
		<div id="flow-chart-container">
			<div id="the-flowchart"></div>
      <i class="fa fa-plus-circle zoomInButton" aria-hidden="true" (click)="zoomInDiv()"></i>
      <i class="fa fa-minus-circle zoomOutButton" aria-hidden="true" (click)="zoomOutDiv()"></i>
      <button class="btn btn-default navbar-btn excelDownloadButton" (click)="downloadExcel()"><i class="fa fa-file-excel-o excelIcon" aria-hidden="true"></i>Download As Excel</button>
    </div>
	`,
  styleUrls: ['../style.css'],
})

export class TheFlowchartComponent {

  TheOperatorNumNow: number;
  currentResult: any;
  currentResultID: string = "";

  constructor(private currentDataService: CurrentDataService) {
    currentDataService.newAddition$.subscribe(
      data => {
        this.TheOperatorNumNow = data.operatorNum;
      }
    );
    currentDataService.checkPressed$.subscribe(
      // used for download as excel button
      data => {
        if (data.code === 0) {
          this.currentResultID = data.resultID;

          jQuery('.excelDownloadButton').css("display","block");
          jQuery('.excelDownloadButton').css({"opacity":"0.8"});
        } else {
          jQuery('.excelDownloadButton').css("display","none");
        }
      }
    );
  }

  downloadExcel() {
    this.currentDataService.downloadExcel(this.currentResultID);
  }


  zoomInDiv(){
    // hide menu when zoomed
    jQuery("#menu").css({
      "display" : "none",
    });
    var FlowChartWidth = jQuery("#the-flowchart").width();
    var FlowChartHeight = jQuery("#the-flowchart").height();
    var matrix = jQuery('#the-flowchart').panzoom("getMatrix");
    var ZoomRatio = parseFloat(matrix[0]);
    // getting the original offset of flowchart
    FlowChartWidth *= ZoomRatio;
    FlowChartHeight *= ZoomRatio;
    if (ZoomRatio < MAX_SCALE){
      ZoomRatio += INCREMENT;
      jQuery('#the-flowchart').flowchart('setPositionRatio', ZoomRatio);
      jQuery('#the-flowchart').panzoom('zoom', ZoomRatio, {
        animate: false,
      });
      var new_width = FlowChartWidth / ZoomRatio;
      var left_side_add = (new_width - FlowChartWidth) / 2 ;
      var new_height = FlowChartHeight / ZoomRatio;
      var top_side_add = (new_height - FlowChartHeight) / 2;
      jQuery("#the-flowchart").css({
        "width" : new_width + "px",
        "left" : -left_side_add + "px",
        "height" : new_height + "px",
        "top" : -top_side_add + "px",
      });
    }
  }

  zoomOutDiv(){
    // hide menu when zoomed
    jQuery("#menu").css({
      "display" : "none",
    });
    var FlowChartWidth = jQuery("#the-flowchart").width();
    var FlowChartHeight = jQuery("#the-flowchart").height();
    console.log(FlowChartWidth);
    var matrix = jQuery('#the-flowchart').panzoom("getMatrix");
    var ZoomRatio = parseFloat(matrix[0]);
    // getting the original offset of flowchart
    FlowChartWidth *= ZoomRatio;
    FlowChartHeight *= ZoomRatio;
    if (ZoomRatio >= MIN_SCALE + INCREMENT){
      ZoomRatio -= INCREMENT;
      jQuery('#the-flowchart').flowchart('setPositionRatio', ZoomRatio);
      jQuery('#the-flowchart').panzoom('zoom', ZoomRatio, {
        animate: false,
      });
      var new_width = FlowChartWidth / ZoomRatio;
      var left_side_add = (new_width - FlowChartWidth) / 2 ;
      var new_height = FlowChartHeight / ZoomRatio;
      var top_side_add = (new_height - FlowChartHeight) / 2;
      jQuery("#the-flowchart").css({
        "width" : new_width + "px",
        "left" : -left_side_add + "px",
        "height" : new_height + "px",
        "top" : -top_side_add + "px",
      });
    }
  }


  initialize(data: any) {
    var current = this;
    var changedOperatorId = -1;

    // unselect operator when user click other div
    jQuery('html').mousedown(function(e) {
      var container = jQuery(".form-control");
      if (container.is(e.target)) {
        jQuery("#the-flowchart").flowchart("unselectOperator");
      }
      // hide the right click menu when click other modules
      var flowchart_container = jQuery("#the-flowchart");
      if (flowchart_container.has(e.target).length <= 0){
      jQuery("#the-flowchart").flowchart("hideRightClickMenu");
      }
    });


    jQuery('html').keyup(function(e) { //key binding function
      if (e.keyCode === 8) { //backspace
        var current_id = jQuery('#the-flowchart').flowchart('getSelectedOperatorId');
        if (current_id !== null) {
          jQuery('#the-flowchart').flowchart('deleteSelected');
          current.currentDataService.clearData();
          current.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
        }
      } else if (e.keyCode === 46) { //delete
        var current_id = jQuery('#the-flowchart').flowchart('getSelectedOperatorId');
        if (current_id !== null) {
          jQuery('#the-flowchart').flowchart('deleteSelected');
          current.currentDataService.clearData();
          current.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
        }
      }
    });

    jQuery('#the-flowchart').flowchart({
      data: data,
      multipleLinksOnOutput: true,
      timer: null,

      // When the user delete an operator, all operators' attributes in
      // the rest of the graph need to be cleaned up
      onLinkDelete: function (linkId, forced) {
        current.currentDataService.clearToOperatorAttribute(linkId);
        return true;
      },
      onOperatorSelect: function(operatorId) {
        current.currentDataService.selectData(operatorId);
        return true;
      },
      onOperatorUnselect: function(operatorId) {
        return true;
      },
      // called when the delete button on the right click menu is clicked
      onRightClickedDelete : function (operatorId) {
        current.currentDataService.clearData();
        current.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
        return true;
      },
      onOperatorCreate: function (operatorId, operatorData, fullElement) {
        changedOperatorId = operatorId;
        return true;
      },
      onAfterChange: function (changeType) {
        clearTimeout(this.timer);
        console.log(changeType);
        // If it's only an "operator_moved" or "operator_create" change,
        // we don't do any update at all. Otherwise, we need to update
        // the graph, which includes "link_create", "link_delete",
        // "operator_data_change", and "operator_delete"
        if (changeType == "operator_data_change") {
            current.currentDataService.clearAllOperatorAttributeFromCurrentSource(changedOperatorId);
        }
        
        this.timer = setTimeout(() => {
            if (changeType != "operator_moved" && changeType != "operator_create") {
                current.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
                current.currentDataService.processAutoPlanData();
            }
        }, 50);
      },
    });

    var FlowChartWidth = parseInt(jQuery("#the-flowchart").css("width"));
    var FlowChartHeight = parseInt(jQuery("#the-flowchart").css("height"));

    var initializePanzoom = this.initializePanzoom;
    initializePanzoom(jQuery('#the-flowchart').parent(), FlowChartWidth, FlowChartHeight);


    var handleResize = function(){
      var currentWindowWidth = jQuery(window).width();
      var currentWindowHeight = jQuery(window).height();
      var currentSideBarWidth = jQuery("#sidebar-wrapper").width();
      var new_width = currentWindowWidth - currentSideBarWidth;
      var new_height = currentWindowHeight - 140; // height of navbar + operatorBar + result bar

      var matrix = jQuery('#the-flowchart').panzoom("getMatrix");
      var ZoomRatio = parseFloat(matrix[0]);

      var newFlowChartWidth = new_width / ZoomRatio;
      var newLeft = (newFlowChartWidth - new_width) / 2;
      var newFlowChartHeight = new_height / ZoomRatio;
      var newTop = (newFlowChartHeight - new_height) / 2;

      jQuery("#the-flowchart").css({
        "width" : newFlowChartWidth + "px",
        "left" : - newLeft + "px",
        "height" : newFlowChartHeight + "px",
        "top" : - newTop + "px",
      });

      jQuery('#the-flowchart').panzoom("reset");
      jQuery('#the-flowchart').panzoom("destroy");
      initializePanzoom(jQuery('#the-flowchart').parent(), new_width, new_height, ZoomRatio);
    }
    window.onresize = handleResize;
  }

  initializePanzoom(container: any, InitialWidth: number, InitialHeight: number, defaultZoom: number = 1) {
    // Panzoom initialization...
    jQuery('#the-flowchart').panzoom({
      disablePan: true, // disable the pan
      // contain : true, // if pan, only can pan within flowchart div
      minScale: MIN_SCALE,
      maxScale: MAX_SCALE,
      increment: INCREMENT,
      startTransform : 'scale('+ defaultZoom + ')',
    });
    var possibleZooms = [];
    for (var i = MIN_SCALE; i < MAX_SCALE; i += INCREMENT) {
      possibleZooms.push(i);
    }
    var currentZoom = 2;
    container.on('mousewheel.focal', function(e) {
      e.preventDefault();
      var delta = (e.delta || e.originalEvent.wheelDelta) || e.originalEvent.detail;
      var zoomOut = delta;
      // var zoomOut = delta ? delta < 0 : e.originalEvent.deltaY > 0;
      currentZoom = Math.max(0, Math.min(possibleZooms.length - 1, (currentZoom + (zoomOut / 40 - 1))));
      var currentZoomRound = Math.round(currentZoom);
      jQuery('#the-flowchart').flowchart('setPositionRatio', possibleZooms[currentZoomRound]);
      jQuery('#the-flowchart').panzoom('zoom', possibleZooms[currentZoomRound], {
        animate: false,
        focal: e
      });
      var ZoomRatio = possibleZooms[currentZoomRound];
      // enlarge the div ratio so there's more space for the operators
      var new_width = InitialWidth / ZoomRatio;
      var left_side_add = (new_width - InitialWidth) / 2 ;

      var new_height = InitialHeight / ZoomRatio;
      var top_side_add = (new_height - InitialHeight) / 2;

      jQuery("#the-flowchart").css({
        "width" : new_width + "px",
        "left" : -left_side_add + "px",
        "height" : new_height + "px",
        "top" : -top_side_add + "px",
      });
      // hide menu when zoomed
      jQuery("#menu").css({
        "display" : "none",
      });
    });
    // panzoom end
  }

}
