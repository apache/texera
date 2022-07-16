from proto.edu.uci.ics.amber.engine.architecture.worker import StoreUpstreamLinkIdsV2
from .handler_base import Handler
from ..managers.context import Context


class StoreUpstreamLinkIdsHandler(Handler):
    cmd = StoreUpstreamLinkIdsV2

    def __call__(self, context: Context, command: StoreUpstreamLinkIdsV2, *args, **kwargs):
        context.batch_to_tuple_converter.update_all_upstream_link_ids(
            set(command.upstream_link_ids)
        )
        return None
