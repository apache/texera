package edu.uci.ics.texera.web.resource.dashboard.hub

sealed trait EntityType {
  def value: String
}

object EntityType {
  case object Workflow extends EntityType { val value = "workflow" }
  case object Dataset extends EntityType { val value = "dataset" }

  val values: Seq[EntityType] = Seq(Workflow, Dataset)

  def fromString(s: String): EntityType =
    values.find(_.value.equalsIgnoreCase(s)).getOrElse {
      throw new IllegalArgumentException(
        s"Invalid entityType '$s'; allowed = ${values.map(_.value).mkString(", ")}"
      )
    }
}
