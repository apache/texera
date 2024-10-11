// root project definition
lazy val MicroServices = (project in file("."))
  .aggregate(TexeraWorkflowCompilingService) // Add all subprojects
  .settings(
    name := "micro-services",
    version := "0.1.0"
  )

// The template of the subproject: TexeraWorkflowCompilingService(as an example)
// lazy val TexeraWorkflowCompilingService = (project in file("texera-workflow-compiling-service"))
//  .settings(
//    name := "TexeraWorkflowCompilingService",
//    version := "0.1.0"
//    libraryDependencies ++= Seq(
//      "io.dropwizard" % "dropwizard-core" % "4.0.7",
//      "com.typesafe" % "config" % "1.4.1",
//      "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.2",       // Jackson Databind for JSON processing
//      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.17.2",    // Jackson Annotations for JSON properties
//      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.2" // Jackson Scala module
//    )
//  )