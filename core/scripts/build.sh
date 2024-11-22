cd amber
sbt clean dist
unzip target/universal/texera-0.1-SNAPSHOT.zip -d target/universal/
rm target/universal/texera-0.1-SNAPSHOT.zip
cd ..
./scripts/build-services.sh
./scripts/gui.sh