cd amber
if [ ! -z $1 ]
then
    target/texera-0.1-SNAPSHOT/bin/computing-unit-master --cluster $1
else
    target/texera-0.1-SNAPSHOT/bin/computing-unit-master
fi