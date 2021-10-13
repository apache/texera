package edu.uci.ics.texera.web.model.http.response

import org.jooq.types.UInteger

case class AccessResponse(uid: UInteger, wid: UInteger, level: String)
