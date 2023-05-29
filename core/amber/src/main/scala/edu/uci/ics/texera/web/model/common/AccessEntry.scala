package edu.uci.ics.texera.web.model.common
import org.jooq.EnumType

case class AccessEntry(email: String, name: String, privilege: EnumType) {}
