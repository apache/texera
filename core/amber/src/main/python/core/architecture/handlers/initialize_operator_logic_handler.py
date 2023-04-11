from loguru import logger

from proto.edu.uci.ics.amber.engine.architecture.worker import InitializeOperatorLogicV2
from .handler_base import Handler
from ..managers.context import Context


class InitializeOperatorLogicHandler(Handler):
    cmd = InitializeOperatorLogicV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        context.operator_manager.initialize_operator(
            command.code, command.is_source, command.output_schema
        )
        context.batch_to_tuple_converter.update_all_upstream_link_ids(
            set(i.link_id for i in command.input_ordinal_mapping)
        )
        context.tuple_processing_manager.input_link_map = \
            {kv.link_id: kv.port_ordinal for kv in command.input_ordinal_mapping}


        for k,v in  context.tuple_processing_manager.input_link_map.items():
            layer_id  = k
            if command.id == layer_id.to:
                context.tuple_processing_manager.my_upstream_id = layer_id.from_
                context.tuple_processing_manager.my_id = layer_id.to

        return None
