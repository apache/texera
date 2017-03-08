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
