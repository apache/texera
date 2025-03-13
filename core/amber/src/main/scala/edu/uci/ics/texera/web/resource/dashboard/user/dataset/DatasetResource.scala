package edu.uci.ics.texera.web.resource.dashboard.user.dataset

import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.Dataset

import org.jooq.EnumType

object DatasetResource {
  // TODO: move these community resource definitions to a centralized package, similar to workflow-core
  case class DashboardDataset(
      dataset: Dataset,
      ownerEmail: String,
      accessPrivilege: EnumType,
      isOwner: Boolean,
      size: Long
  )
}
