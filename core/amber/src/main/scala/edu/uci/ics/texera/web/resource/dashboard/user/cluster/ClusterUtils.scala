package edu.uci.ics.texera.web.resource.dashboard.user.cluster

import java.sql.Timestamp
import java.time.Instant
import org.jooq.impl.DSL.max
import edu.uci.ics.texera.web.model.jooq.generated.enums.ClusterStatus
import edu.uci.ics.texera.web.model.jooq.generated.tables.Cluster.CLUSTER
import edu.uci.ics.texera.web.model.jooq.generated.tables.ClusterActivity.CLUSTER_ACTIVITY
import org.jooq.DSLContext

object ClusterUtils {

  /**
    * Updates the status of a cluster.
    *
    * @param clusterId The ID of the cluster.
    * @param status The new status of the cluster.
    */
  def updateClusterStatus(clusterId: Int, status: ClusterStatus, context: DSLContext): Unit = {
    context
      .update(CLUSTER)
      .set(CLUSTER.STATUS, status)
      .where(CLUSTER.CID.eq(clusterId))
      .execute()
  }

  /**
    * Updates the end time of the most recent cluster activity to the current time.
    *
    * @param clusterId The ID of the cluster.
    */
  def updateClusterActivityEndTime(clusterId: Int, context: DSLContext): Unit = {
    context
      .update(CLUSTER_ACTIVITY)
      .set(CLUSTER_ACTIVITY.END_TIME, Timestamp.from(Instant.now()))
      .where(CLUSTER_ACTIVITY.CLUSTER_ID.eq(clusterId))
      .and(
        CLUSTER_ACTIVITY.START_TIME.eq(
          context
            .select(max(CLUSTER_ACTIVITY.START_TIME))
            .from(CLUSTER_ACTIVITY)
            .where(CLUSTER_ACTIVITY.CLUSTER_ID.eq(clusterId))
            .and(CLUSTER_ACTIVITY.END_TIME.isNull)
        )
      )
      .execute()
  }
}
