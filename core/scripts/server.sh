pylsp --ws --port 3000 &
cd amber
if [ ! -z $1 ] 
then 
    target/texera-0.1-SNAPSHOT/bin/texera-web-application  --cluster $1
else
    target/texera-0.1-SNAPSHOT/bin/texera-web-application
fi