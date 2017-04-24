import { Component } from '@angular/core';

import { MockDataService } from './mock-data-service';
import { CurrentDataService } from './current-data-service';

declare var jQuery: any;

@Component({
    moduleId: module.id,
    selector: '[operator-bar]',
    templateUrl: './operator-bar.component.html',
    styleUrls: ['style.css']
})
export class OperatorBarComponent {

    constructor(private mockDataService: MockDataService, private currentDataService: CurrentDataService) { }

    initialize(){
        var container = jQuery('#the-flowchart').parent();
        var draggableOperators = jQuery('.draggable_operator');


        // // panzoom begin

        // jQuery('#the-flowchart').panzoom({
        //   contain: true,
        //   animate: false,
        //   disablePan: true,
        //   $zoomIn: jQuery('.zoom-in'),
        //   $zoomOut: jQuery('.zoom-out'),
        //   $reset: jQuery('.reset'),
        // });
        // jQuery('#the-flowchart').panzoom('pan',-cx + container.width() /2, -cy + container.height()/2);

        var cx = jQuery('#the-flowchart').width() / 2;
        var cy = jQuery('#the-flowchart').height() / 2;
        // Panzoom initialization...
        jQuery('#the-flowchart').panzoom({
          disablePan: true, // disable the pan
          // contain : true, // if pan, only can pan within flowchart div

        });
        var possibleZooms = [0.7, 0.8, 0.9, 1];
        var currentZoom = 2;
        container.on('mousewheel.focal', function( e ) {
            e.preventDefault();
            var delta = (e.delta || e.originalEvent.wheelDelta) || e.originalEvent.detail;
            var zoomOut = delta;
            // var zoomOut = delta ? delta < 0 : e.originalEvent.deltaY > 0;
            currentZoom = Math.max(0, Math.min(possibleZooms.length - 1, (currentZoom + (zoomOut/40 - 1))));
            jQuery('#the-flowchart').flowchart('setPositionRatio', possibleZooms[currentZoom]);
            jQuery('#the-flowchart').panzoom('zoom', possibleZooms[currentZoom], {
                animate: false,
                focal: e
            });
        });
        // // panzoom end


        var current = this;
        current.mockDataService.getMatchers().then(
            matchers => {
                draggableOperators.draggable({
                    cursor: "move",
                    opacity: 0.7,

                    appendTo: 'body',
                    zIndex: 1000,

                    helper: function(e) {
                        var dragged = jQuery(this);
                        var matcherId = parseInt(dragged.data('matcher-type'));
                        var data = matchers[matcherId].jsonData;

                        return jQuery('#the-flowchart').flowchart('getOperatorElement', data);
                    },
                    stop: function(e, ui) {
                        var dragged = jQuery(this);
                        var matcherId = parseInt(dragged.data('matcher-type'));
                        var data = matchers[matcherId].jsonData;

                        var elOffset = ui.offset;
                        var containerOffset = container.offset();
                        if (elOffset.left > containerOffset.left &&
                            elOffset.top > containerOffset.top &&
                            elOffset.left < containerOffset.left + container.width() &&
                            elOffset.top < containerOffset.top + container.height()) {

                            var flowchartOffset = jQuery('#the-flowchart').offset();

                            var relativeLeft = elOffset.left - flowchartOffset.left;
                            var relativeTop = elOffset.top - flowchartOffset.top;

                            var positionRatio = jQuery('#the-flowchart').flowchart('getPositionRatio');
                            relativeLeft /= positionRatio;
                            relativeTop /= positionRatio;

                            data.left = relativeLeft;
                            data.top = relativeTop;

                            var operatorNum = jQuery('#the-flowchart').flowchart('addOperator', data);
                            current.currentDataService.addData(data, operatorNum, jQuery('#the-flowchart').flowchart('getData'));
                        }
                    }
                });
            },
            error => {
                console.log(error);
            }
        );
    }
}
