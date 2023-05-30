cd amber
if [ ! -z $1 ] 
then 
    sbt "runMain edu.uci.ics.texera.web.TexeraWebApplication --cluster $1" 
else
    sbt "runMain edu.uci.ics.texera.web.TexeraWebApplication" 
fi
