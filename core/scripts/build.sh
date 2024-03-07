cd amber
sbt clean dist
unzip target/universal/texera-0.1-SNAPSHOT.zip -d target/universal/
cd ..
./scripts/gui.sh
