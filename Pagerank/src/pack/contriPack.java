package pack;

import java.io.IOException;
//import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class contriPack {

    public static class contriMapper extends Mapper<Object, Text, Text, Text> {

        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
        	//take the data of the joined file
        	StringBuilder joincontent=new StringBuilder(value.toString().split("\t")[1]);
        	//if there are links to this node, do the following; else do nothing
        	if (joincontent.toString().contains("@")){        		
        		String rank=joincontent.substring(joincontent.indexOf("@")+1,joincontent.lastIndexOf("@"));
        		joincontent.delete(joincontent.indexOf("@"),joincontent.lastIndexOf("@")+1);
        		//if there are links from this node,do the following; else do nothing
        		if(!joincontent.toString().equals(""))
        		{
        				String []line=joincontent.toString().split(",");
        				double contri= Double.parseDouble(rank)/(line.length);
        				for (int i=0;i<line.length ;i++){
        					context.write(new Text(line[i]),new Text(String.valueOf(contri)));
        		}	
        	}
       	 }
     	}//end map
                
    }//end mapper
    
    public static class contriReducer extends Reducer<Text, Text, Text, Text> {
    	double sum;
        protected void reduce(Text key, Iterable<Text> values,
                Reducer<Text, Text, Text, Text>.Context context)
                        throws IOException, InterruptedException {
            sum =0;
            for (Text value : values) {
            	sum+=Double.parseDouble(value.toString());
            }
            //damp coefficient=0.85
            context.write(new Text(key.toString()),new Text("@"+String.valueOf(sum*0.85+0.15)+"@"));
        }
    }
    
    
    
}
