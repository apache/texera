cd amber
case $# in
  1)
    echo "running worker with cluster mode"
    sbt "runMain edu.uci.ics.texera.web.TexeraRunWorker --serverAddr $1" 
    ;;

  2)
    echo "running worker with cluster mode and specific memory allocation = $2 MB"
    sbt -mem $2 "runMain edu.uci.ics.texera.web.TexeraRunWorker --serverAddr $1" 
    ;;

  *)
    echo "running worker with local mode"
    sbt "runMain edu.uci.ics.texera.web.TexeraRunWorker" 
    ;;
esac