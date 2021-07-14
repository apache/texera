addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")

// for scalapb code gen
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.3")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.1"
