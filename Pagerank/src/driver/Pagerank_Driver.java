package driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pack.preparePack;
import pack.IntePack;
import pack.SortPack;
import pack.contriPack;
public class Pagerank_Driver {

    private static String sourceinfoPath = "";//the sourcefile of soc-Epinions1.txt 
    private static String linkpagesPath = ""; //the file describes the linkpages
    private static String rankPath = ""; //the result of pagerank
    
    public static void main(String[] args) {
    	Pagerank_Driver client = new Pagerank_Driver();
        
        if (args.length == 3) {
        	sourceinfoPath = args[0];
        	linkpagesPath = args[1];
        	rankPath = args[2];
        }
        
        try {
            client.execute();
        } catch (Exception e) {
            System.err.println(e);
        }
    }
    
    private void execute() throws Exception {

    	 String tempjoinPath="tempJoinPath";   	
         runprepareJob(sourceinfoPath, linkpagesPath,rankPath+"0");
         //do 30 times of calculation and for ith time save the result into "ranki"
    	 for (int i=0;i<30;i++){
    		 //join the data of "linkpages" and the last result of "rank"
    		 runIntegrateJob(linkpagesPath, rankPath+String.valueOf(i), tempjoinPath);
    		 runcontriJob(tempjoinPath,rankPath+String.valueOf(i+1));
    	}
    	 //sort the result of "rank" in descending order
        runSortJob(rankPath+"30",rankPath+"30_sort");
    }
    
    private int runprepareJob(String inputPath, String outputPathA, String outputPathB) throws Exception {
        Configuration configuration1 = new Configuration();
        remove_ifexist(configuration1, outputPathA);
        
        Job job1 = Job.getInstance(configuration1);
        job1.setJobName("Job_linkpages_tmp");
        job1.setJarByClass(preparePack.class);
        job1.setMapperClass(preparePack.prepareMapper1.class);
        job1.setReducerClass(preparePack.prepareReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPathA)); 
        
        job1.waitForCompletion(true);
        
        Configuration configuration2 = new Configuration();
        remove_ifexist(configuration2, outputPathB);
        
        Job job2 = Job.getInstance(configuration2);
        job2.setJobName("Job_rank_tmp");
        job2.setJarByClass(preparePack.class);
        job2.setMapperClass(preparePack.prepareMapper2.class);
        job2.setReducerClass(preparePack.prepareReducer2.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(outputPathA));
        FileOutputFormat.setOutputPath(job2, new Path(outputPathB));      
               
        return job2.waitForCompletion(true) ? 0 : 1;             
    }

    
    private int runIntegrateJob (String inputPathA, String inputPathB, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        remove_ifexist(configuration, outputPath);
        
        Job job = Job.getInstance(configuration);
        job.setJobName("Job_Combine");
        job.setJarByClass(IntePack.class);
        
        job.setMapperClass(IntePack.InteMapper.class);
        job.setReducerClass(IntePack.InteReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPathA));
        FileInputFormat.addInputPath(job, new Path(inputPathB));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    private int runcontriJob(String inputPath, String outputPath) throws Exception {
        Configuration configuration = new Configuration();
        remove_ifexist(configuration, outputPath);
        
        Job job = Job.getInstance(configuration);
        job.setJobName("Job_Contribution");
        job.setJarByClass(SortPack.class);
        
        job.setMapperClass(contriPack.contriMapper.class);
        job.setReducerClass(contriPack.contriReducer.class);
             
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
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

}
