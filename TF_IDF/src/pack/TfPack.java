package pack;

import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;


//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TfPack {

    public static class TfMapper1 extends Mapper<Object, Text, Text, Text> {
        
        private final Text one = new Text("1");
        private Text word = new Text();
        private String fileName = "";
        
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {        	
            fileName = ((FileSplit)context.getInputSplit()).getPath().getName();            
        }

        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
        	String line=value.toString();
        	line=line.replaceAll("[^a-zA-Z ]", " ").toLowerCase();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken()+":"+fileName);
                context.write(word, one);
            }
        }
                
    }
    
    public static class TfReducer1 extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values,
                Reducer<Text, Text, Text, Text>.Context context)
                        throws IOException, InterruptedException {
            int sum =0;
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                sum += Integer.parseInt(iterator.next().toString());
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }
    
    public static class TfMapper2 extends Mapper<Object, Text, Text, Text> {
        private Text doc = new Text();
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            String[] line=value.toString().split(":|\t");
            //System.out.println(line[2]);
            doc.set(line[1]);
            context.write(doc, new Text(line[0]+","+line[2]));
        }
                
    }
    public static class TfReducer2 extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values,
                Reducer<Text, Text, Text, Text>.Context context)
                        throws IOException, InterruptedException {
            int sum =0;
            Map<String,String> temp = new HashMap<String,String>();
            for(Text val: values){
            	String[] line=val.toString().split(",");
            	temp.put(line[0], line[1]);
            	sum+=Integer.parseInt(line[1]);
            }
            
            for(String word: temp.keySet()){
            	double tf=Double.parseDouble(temp.get(word))/sum;
            	context.write(new Text(word+":"+key.toString()),new Text(String.valueOf(tf)));
            }
        }
    }
    
    
}
