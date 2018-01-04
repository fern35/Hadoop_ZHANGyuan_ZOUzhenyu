package pack;

import java.io.IOException;
//import java.util.Iterator;
//import java.util.StringTokenizer;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class preparePack {
    public static class prepareMapper1 extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
        	String line=value.toString();
        	if (!line.startsWith("#")){
        		String []index=line.split("\t");
        		context.write(new Text(index[0]), new Text(index[1]));
        	}
        }
                
    }
    
    public static class prepareReducer1 extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values,
                Reducer<Text, Text, Text, Text>.Context context)
                        throws IOException, InterruptedException {
        	StringBuilder linkpages=new StringBuilder("");
            for (Text value : values) {
            	linkpages.append(value+",");
            }
            linkpages.deleteCharAt(linkpages.length()-1);
            context.write(new Text(key.toString()), new Text(linkpages.toString()));
        }
    }
    public static class prepareMapper2 extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
        	String line=value.toString();
        		String []index=line.split("\t");
        		context.write(new Text(index[0]), new Text("@1@"));
        }               
    }
    
    public static class prepareReducer2 extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values,
                Reducer<Text, Text, Text, Text>.Context context)
                        throws IOException, InterruptedException {
        	for( Text value:values){
        		context.write(new Text(key.toString()), value);        		
        	}
        }
    }    

}
