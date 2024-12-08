cd amber
if [ ! -z $1 ]
then 
    target/texera-0.1-SNAPSHOT/bin/texera-run-worker --serverAddr $1
else
    target/texera-0.1-SNAPSHOT/bin/texera-run-worker
fi
