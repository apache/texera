import { Component, ViewChild  } from '@angular/core';
import { ModalComponent } from 'ng2-bs3-modal/ng2-bs3-modal';
declare var jQuery: any;

@Component({moduleId: module.id,
  selector: '[operator-bar]',
  templateUrl: './operator-bar.component.html'
})
export class OperatorBarComponent {
    @ViewChild('modal') modal: ModalComponent;

    close() {
        this.modal.close();
    }

    open() {
        this.modal.open();
    }

    ngAfterViewInit() {
        var current = this;
        var container = jQuery('#the-flowchart').parent();
        var draggableOperators = jQuery('.draggable_operator');

        var operatorId = 0;

        draggableOperators.draggable({
            cursor: "move",
            opacity: 0.7,

            appendTo: 'body',
            zIndex: 1000,

            helper: function(e) {
                var dragged = jQuery(this);
                var nbInputs = parseInt(dragged.data('nb-inputs'));
                var nbOutputs = parseInt(dragged.data('nb-outputs'));
                var data = {
                    properties: {
                        title: dragged.text(),
                        inputs: {},
                        outputs: {}
                    }
                };

                var i = 0;
                for (i = 0; i < nbInputs; i++) {
                    data.properties.inputs['input_' + i] = {
                        label: 'Input ' + (i + 1)
                    };
                }
                for (i = 0; i < nbOutputs; i++) {
                    data.properties.outputs['output_' + i] = {
                        label: 'Output ' + (i + 1)
                    };
                }

                return jQuery('#the-flowchart').flowchart('getOperatorElement', data);
            },
            stop: function(e, ui) {
                current.open();
                var dragged = jQuery(this);
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

                    var nbInputs = parseInt(dragged.data('nb-inputs'));
                    var nbOutputs = parseInt(dragged.data('nb-outputs'));
                    var data = {
                        left: relativeLeft,
                        top: relativeTop,
                        properties: {
                            title: dragged.text(),
                            inputs: {},
                            outputs: {}
                        }
                    };

                    var i = 0;
                    for (i = 0; i < nbInputs; i++) {
                        data.properties.inputs['input_' + i] = {
                            label: 'Input ' + (i + 1)
                        };
                    }
                    for (i = 0; i < nbOutputs; i++) {
                        data.properties.outputs['output_' + i] = {
                            label: 'Output ' + (i + 1)
                        };
                    }


                    jQuery('#the-flowchart').flowchart('addOperator', data);
                }
            }
        });
    }

    initialize(){
        var container = jQuery('#the-flowchart').parent();
        var draggableOperators = jQuery('.draggable_operator');

        var operatorId = 0;

        draggableOperators.draggable({
            cursor: "move",
            opacity: 0.7,

            appendTo: 'body',
            zIndex: 1000,

            helper: function(e) {
                var dragged = jQuery(this);
                var nbInputs = parseInt(dragged.data('nb-inputs'));
                var nbOutputs = parseInt(dragged.data('nb-outputs'));
                var data = {
                    properties: {
                        title: dragged.text(),
                        inputs: {},
                        outputs: {}
                    }
                };

                var i = 0;
                for (i = 0; i < nbInputs; i++) {
                    data.properties.inputs['input_' + i] = {
                        label: 'Input ' + (i + 1)
                    };
                }
                for (i = 0; i < nbOutputs; i++) {
                    data.properties.outputs['output_' + i] = {
                        label: 'Output ' + (i + 1)
                    };
                }

                return jQuery('#the-flowchart').flowchart('getOperatorElement', data);
            },
            stop: function(e, ui) {
                this.modal.open();
                var dragged = jQuery(this);
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

                    var nbInputs = parseInt(dragged.data('nb-inputs'));
                    var nbOutputs = parseInt(dragged.data('nb-outputs'));
                    var data = {
                        left: relativeLeft,
                        top: relativeTop,
                        properties: {
                            title: dragged.text(),
                            inputs: {},
                            outputs: {}
                        }
                    };

                    var i = 0;
                    for (i = 0; i < nbInputs; i++) {
                        data.properties.inputs['input_' + i] = {
                            label: 'Input ' + (i + 1)
                        };
                    }
                    for (i = 0; i < nbOutputs; i++) {
                        data.properties.outputs['output_' + i] = {
                            label: 'Output ' + (i + 1)
                        };
                    }


                    jQuery('#the-flowchart').flowchart('addOperator', data);
                }
            }
        });
    }
}
