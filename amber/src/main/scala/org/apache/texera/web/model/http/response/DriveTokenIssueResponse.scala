package org.apache.texera.web.model.http.response

case class DriveTokenIssueResponse(
    status: String,
    accessToken: Option[String]
)