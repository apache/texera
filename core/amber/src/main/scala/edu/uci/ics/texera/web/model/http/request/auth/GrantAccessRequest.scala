package edu.uci.ics.texera.web.model.http.request.auth

case class GrantAccessRequest(
    username: String, // the file name of target file to be shared
    fileName: String, // the username of target user to be shared to
    ownerName: String, // the name of the file's owner
    accessLevel: String // the type of access to be shared
)
