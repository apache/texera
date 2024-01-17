from proto.edu.uci.ics.amber.engine.architecture.worker import InitializeOperatorLogicV2
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context


class InitializeOperatorLogicHandler(ControlHandler):
    cmd = InitializeOperatorLogicV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        context.operator_manager.initialize_operator(
            command.code, command.is_source, command.output_schema
        )
        context.batch_to_tuple_converter.update_all_upstream_links(
            set(link_ordinal.link for link_ordinal in command.input_ordinal_mapping)
        )
        context.tuple_processing_manager.input_link_map = {
            link_ordinal.link: link_ordinal.port_ordinal
            for link_ordinal in command.input_ordinal_mapping
        }
        return None
