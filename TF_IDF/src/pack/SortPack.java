package pack;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparator; 

public class SortPack {

    public static class SortMapper extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String str[]=value.toString().split("\t");
            context.write(new Text(str[1]), new Text(str[0]));
        }
    }
    
    public static class SortReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text label = new Text();

        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            for(Text val : values){  
                label.set(val);  
                context.write(label, key);  
            }
        }
    }
    
    public static class DescComparator extends WritableComparator{  
    	  
        protected DescComparator() {  
            super(Text.class,true);  
        }  

        public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,  
                int arg4, int arg5) {  
            return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);  
        }   
        public int compare(Object a,Object b){  
            return -super.compare(a, b);  
        }  
    } 
    
    
}
