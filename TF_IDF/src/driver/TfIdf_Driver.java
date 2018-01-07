package driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pack.IdfPack;
import pack.IntePack;
import pack.SortPack;
import pack.TfPack;

public class TfIdf_Driver {

    private static String inputPath = "";
    private static String outputPath = "";

    
    public static void main(String[] args) {
    	TfIdf_Driver client = new TfIdf_Driver();
        
        if (args.length == 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        
        try {
            client.execute();
        } catch (Exception e) {
            System.err.println(e);
        }
    }
    
    private void execute() throws Exception {
        String tmpTFPath = outputPath + "_tf";
        String tmpIDFPath = outputPath + "_idf";
        //calculate Tf
        runTFJob(inputPath, tmpTFPath);
        //calulate Idf
        runIDFJob(tmpTFPath, tmpIDFPath,inputPath);
        //combine Tf-Idf
        runIntegrateJob(tmpTFPath, tmpIDFPath, outputPath);
        //Sort the result
        runSortJob(outputPath,outputPath+"_sort");
    }
    
    private int runTFJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration1 = new Configuration();
        remove_ifexist(configuration1, outputPath+"_tmp");
        
        Job job1 = Job.getInstance(configuration1);
        job1.setJobName("Job_Tf1");
        job1.setJarByClass(TfPack.class);

        job1.setMapperClass(TfPack.TfMapper1.class);
        job1.setNumReduceTasks(getNumReduceTasks(configuration1, inputPath));
        job1.setReducerClass(TfPack.TfReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath+"_tmp"));
        
        job1.waitForCompletion(true);
        
        //-----------------part 2-------------------
        Configuration configuration2 = new Configuration();
        remove_ifexist(configuration2, outputPath);
        
        Job job2 = Job.getInstance(configuration2);
        job2.setJobName("Job_Tf2");
        job2.setJarByClass(TfPack.class);

        job2.setMapperClass(TfPack.TfMapper2.class);
        job2.setReducerClass(TfPack.TfReducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(outputPath+"_tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));
        
        return job2.waitForCompletion(true) ? 0 : 1;
    }
    
    private int runIDFJob(String inputPath, String outputPath,String tfinputPath1) throws Exception {
        Configuration configuration = new Configuration();
        remove_ifexist(configuration, outputPath);
        
        Job job = Job.getInstance(configuration);
        job.setJobName("Job_Idf");
        job.setJarByClass(IdfPack.class);
        
        job.setMapperClass(IdfPack.IdfMapper.class);
        job.setReducerClass(IdfPack.IdfReducer.class);
        job.setProfileParams(String.valueOf(getNumReduceTasks(configuration,tfinputPath1)));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private int runIntegrateJob (String inputTFPath, String inputIDFPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        remove_ifexist(configuration, outputPath);
        
        Job job = Job.getInstance(configuration);
        job.setJobName("Job_CombineTfIdf");
        job.setJarByClass(IntePack.class);
        
        job.setMapperClass(IntePack.InteMapper.class);
        job.setReducerClass(IntePack.InteReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputTFPath));
        FileInputFormat.addInputPath(job, new Path(inputIDFPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private int runSortJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        remove_ifexist(configuration, outputPath);
        
        Job job = Job.getInstance(configuration);
        job.setJobName("Job_Sort");
        job.setJarByClass(SortPack.class);
        
        job.setMapperClass(SortPack.SortMapper.class);
        job.setReducerClass(SortPack.SortReducer.class);
       
        job.setSortComparatorClass(SortPack.DescComparator.class);          
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private void remove_ifexist(Configuration configuration, String outputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(outputPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }
    
    private int getNumReduceTasks(Configuration configuration, String inputPath) throws Exception {
        FileSystem hdfs = FileSystem.get(configuration);
        FileStatus status[] = hdfs.listStatus(new Path(inputPath));
        return status.length;
    }
}
