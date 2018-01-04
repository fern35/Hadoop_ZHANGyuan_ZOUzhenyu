package pack;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class IntePack {

    public static class InteMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            //list all the data of the result of "Tf" and "Idf"
            context.write(new Text(tokenizer.nextToken()), new Text(tokenizer.nextToken()));
        }
    }
    
    public static class InteReducer extends Reducer<Text, Text, Text, Text> {
        
        private double keywordIDF = 0.0;
        private Text value = new Text();

        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            //detect the value of Idf
            if (key.toString().split(":")[1].startsWith("!")) {
                keywordIDF = Double.parseDouble(values.iterator().next().toString());
                return;
            }
            //combine the value of Tf-Idf
            value.set(String.valueOf(Double.parseDouble(values.iterator().next().toString()) * keywordIDF));
            
            context.write(key, value);
        }
    }
}