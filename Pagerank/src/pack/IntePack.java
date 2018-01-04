package pack;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class IntePack {

    public static class InteMapper extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            //list all the data of two files into one file
            context.write(new Text(tokenizer.nextToken()), new Text(tokenizer.nextToken()));
        }
    }
    
    public static class InteReducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
        	//save all the data of two files into one file
        	StringBuilder joincontent=new StringBuilder("");
            for (Text val: values){
            	joincontent.append(val);
            }   
            context.write(key, new Text(joincontent.toString()));
        }
    }
}
