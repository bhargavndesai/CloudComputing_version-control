import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
/*import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;*/
import org.json.*;


public class wordFrequnecy {

  public static class FreqMap extends Mapper<Object, Text, Text, LongWritable> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
   //String userName[];
   int twtLength;
   //JSONObject jsonObject = null;
    //for (String line : value.toString)
   //{    
		JSONObject jsonObject = new JSONObject(value.toString());

		//String name = (String) jsonObject.get("name");
		
   //   String[] words = value.toString().split(" "); 
   //   for( String word : words){
   
   //userName.add[jsonObject.getString("user").getString["screen_name");
   //twtlength.add(jsonObject.getStirng("text").length());
   twtLength=jsonObject.getString("text").length();
   //jsonObject=null;
    
   // }
        context.write(new Text((String)jsonObject.getString("id_str")), new LongWritable(twtLength));
      //}
      
    }
  }

  public static class FreqReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      
      long temp = 0;
    Map<String,Integer> twtLen= new HashMap<String,Integer>();
      for (LongWritable value : values) {
      temp=temp+1;
         
      }
      
      context.write(key, new LongWritable(temp));
    }
  }
  /*public static class MostTwtUser extends Mapper<Text, LongWritable, Text, LongWritable> {

    public void map(Text key,  Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
        List list = new ArrayList();
        
    //  Map<String,Integer> twtLen= new HashMap<String,Integer>
      for (LongWritable value : values) {
      list.add[values.get()];      
        
      }
      Collections.sort(list);
      for(int i=0;i<list.size();i++)
      {
      context.write(key, new LongWritable(list[i]));
      
    }
  }*/


  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: wordFrequnecy <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(wordFrequnecy.class);
    job.setJobName("wordFrequnecy");
    job.setNumReduceTasks(3);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(FreqMap.class);
    job.setReducerClass(FreqReduce.class);
    job.setCombinerClass(FreqReduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
