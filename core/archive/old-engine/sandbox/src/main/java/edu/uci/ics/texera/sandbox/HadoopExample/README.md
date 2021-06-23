This example is to evaluate if we could use hadoop as runtime engine. 

It is a simple test to simulate texera operator with hadoop map-reduce.However, there are lots of work to do, such as query plan optimization, table metedata maintain.

In the example, we do the following operation:
Scan -->  KeywordMatch (on text)--> Count (on create date)-->Filter(on create date)--> ViewResult

You could follow the steps to run:
1. Download Hadoop from http://hadoop.apache.org/releases.html
2. Configure hadoop following the instruction http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html
3. Put the data file tweets.json to hdfs.
4. Run the application and specify the input/output path
5. Find result in output path 

You could also bulid a jar and run the jar file in hadoop:
1. Put jar file under (hadoop_dir)/share/hadoop/mapreduce/
2. Run command ./bin/hadoop jar ./share/hadoop/mapreduce/sandbox.jar HadoopExample [input path] [output path]
3. Find result in output path. 

 
Possible problems:
1. If you use a mac and run with hadoop command, you should use following command to remove liscense file:
zip -d ./share/hadoop/mapreduce/sandbox.jar LICENSE
zip -d ./share/hadoop/mapreduce/sandbox.jar META-INF/LICENSE
