// root project definition
lazy val MicroServices = (project in file("."))
  .settings(
    name := "micro-services",
    version := "0.1.0"
  )

// The template of the subproject: WorkflowCompilingService(as an example)
// lazy val WorkflowCompilingService = (project in file("workflow-compiling-service"))
//  .settings(
//    name := "WorkflowCompilingService",
//    version := "0.1.0"
//    libraryDependencies ++= Seq(
//      "io.dropwizard" % "dropwizard-core" % "4.0.7",
//      "com.typesafe" % "config" % "1.4.1",
//      "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.2",       // Jackson Databind for JSON processing
//      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.17.2",    // Jackson Annotations for JSON properties
//      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.2" // Jackson Scala module
//    )
//  )
// once this subproject is defined, aggregate it to the MicroServices definition