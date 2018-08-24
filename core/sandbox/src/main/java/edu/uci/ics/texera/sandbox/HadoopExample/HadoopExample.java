package edu.uci.ics.texera.sandbox.HadoopExample;

import edu.uci.ics.texera.sandbox.HadoopExample.mr.QueryPlan;
import edu.uci.ics.texera.sandbox.HadoopExample.mr.TexeraMapper;
import edu.uci.ics.texera.sandbox.HadoopExample.mr.TexeraReducer;
import edu.uci.ics.texera.sandbox.HadoopExample.utils.OperatorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;


public class HadoopExample extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {

        // hard code query plan
        String queryPlan = "{\"operators\":[{\"operatorID\":\"0\",\"operatorType\":\"ScanSource\",\"tableName\":\"01_twitter_climate_change\"}," +
                "{\"operatorID\":\"1\",\"operatorType\":\"KeywordMatcher\",\"attributes\":\"text\",\"query\":\"climate\",\"luceneAnalyzer\":\"standard\",\"matchingType\":\"phrase\",\"spanListName\":\" \"}," +
                "{\"operatorID\":\"2\",\"operatorType\":\"Count\",\"attributes\":\"create_at\"}," +
                "{\"operatorID\":\"3\",\"operatorType\":\"Filter\",\"comp\":\"GT\",\"query\":\"2016-08-10\",\"luceneAnalyzer\":\"standard\",\"matchingType\":\"phrase\",\"spanListName\":\" \"}," +
                "{\"operatorID\":\"4\",\"operatorType\":\"ViewResults\",\"limit\":10,\"offset\":0}],\"links\":[{\"origin\":\"0\",\"destination\":\"1\"},{\"origin\":\"1\",\"destination\":\"2\"}]}";


        List<ControlledJob> jobPool = new ArrayList<>();
        List<QueryPlan> queryPlans = OperatorUtils.buildQueryPlan(queryPlan);
        JobControl jobControl = new JobControl("job_chain");

        //file path
        Path inputPath = new Path(args[0]);
        Path outputPath;
        Path outputPathFinal = new Path(args[1]);

        for(int i=0; i<queryPlans.size();i++){
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            FileSystem fs = FileSystem.get(conf);
            conf.set("mapplan", queryPlans.get(i).mapPlan);
            conf.set("reduceplan", queryPlans.get(i).reducePlan);
            Job job = Job.getInstance(conf, "Test");
            job.setJarByClass(HadoopExample.class);
            job.setMapperClass(TexeraMapper.class);
            if(i==0) {
                job.setInputFormatClass(TextInputFormat.class);
            }else{
                job.setInputFormatClass(KeyValueTextInputFormat.class);
            }
            job.setReducerClass(TexeraReducer.class);
            if(queryPlans.get(i).reducePlan.equals("")) {
                job.setNumReduceTasks(0);
            }
            else{
               job.setNumReduceTasks(1);
            }
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, inputPath);
            if(i < queryPlans.size()-1) {
                outputPath = new Path("/output/temp"+i);
                if(fs.exists(outputPath)) {
                    fs.delete(outputPath, true);
                }
                FileOutputFormat.setOutputPath(job, outputPath);
                inputPath = outputPath;
            }else{
                if(fs.exists(outputPathFinal)) {
                    fs.delete(outputPathFinal, true);
                }
                FileOutputFormat.setOutputPath(job, outputPathFinal);
            }
            ControlledJob controlledJob = new ControlledJob(conf);
            controlledJob.setJob(job);
            if(i>0){
                controlledJob.addDependingJob(jobPool.get(i-1));
            }
            jobControl.addJob(controlledJob);
            jobPool.add(controlledJob);
        }
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();
        while(!jobControl.allFinished()){
            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
            System.out.println();
            try{
                Thread.sleep(5000);
            }catch ( Exception e){
                System.out.println("Error");
            }
        }
        System.out.println(jobPool.get(0).getMessage());
        System.out.println(jobControl.getFailedJobList());
        return 0;
    }
    public static void main(String[] args) throws Exception {
        HadoopExample example = new HadoopExample();
        int rc = ToolRunner.run(example,args);
        System.exit(rc);
    }
}
