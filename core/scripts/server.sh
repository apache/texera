cd amber
export SBT_OPTS="-Xmx31G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M  -Duser.timezone=GMT"
sudo nohup sbt -mem 31744 "runMain edu.uci.ics.texera.web.TexeraWebApplication false" > ~/server.log 2>&1 &
echo "success"