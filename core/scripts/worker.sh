if [ ! -z $1 ]
cd amber
then 
    target/texera-0.1-SNAPSHOT/bin/texera-run-worker --serverAddr $1
else
    target/texera-0.1-SNAPSHOT/bin/texera-run-worker
fi
