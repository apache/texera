sbt clean dist
unzip workflow-compiling-service/target/universal/workflow-compiling-service-0.1.0.zip -d target/
rm workflow-compiling-service/target/universal/workflow-compiling-service-0.1.0.zip

unzip amber/target/universal/texera-0.1-SNAPSHOT.zip -d amber/target/
rm amber/target/universal/texera-0.1-SNAPSHOT.zip
