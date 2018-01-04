package pack;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class IdfPack {

    public static class IdfMapper extends Mapper<Object, Text, Text, Text> {
        
        private final Text one = new Text("1");
        private Text word = new Text();

        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String line[]=value.toString().split(":|\t");
            word.set(line[0]);
            //write(word:filename, 1)
            context.write(word, one);
        }
    }
    
    public static class IdfReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text label = new Text();
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            int fileCount = 0;
            for (Text value : values) {
                fileCount += Integer.parseInt(value.toString());
            }
            label.set( key.toString()+":"+"!"); 
            //get the total no of files from getProfileParams()
            int totalFileCount = Integer.parseInt(context.getProfileParams());
            //calculate Tf-Idf
            double idfValue = Math.log10(1.0 * totalFileCount / (fileCount));
            
            context.write(label, new Text(String.valueOf(idfValue)));
        }
    }
}
