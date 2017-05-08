cd $TEXTDB_HOME
mvn clean install -DskipTests
./textdb-scripts/gui.sh
