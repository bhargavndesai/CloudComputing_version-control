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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class wordFrequency {

  public static class FreqMap extends Mapper<Object, Text, Text,LongWritable> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
       			long len=0;
        String[] words = value.toString().split(" "); 
        for(int i=0;i<words.length;i++){
        len=words[0].length();
      context.write(new Text(words[1]), new LongWritable(len));  
      }
      
    }
  }

public static class Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long count=0;
      long temp = 0;
      for (LongWritable value : values) {
        temp = value.get();
        count=count+1;
      }
      System.out.println("The count is:"+count+"and the key is :"+key.toString());
      context.write(new Text(key+":"+count), new LongWritable(temp));
    }
  }


  public static class FreqReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      
      long temp = 0;
      String[] tmp=key.toString().split(":");
      int denom=Integer.parseInt(tmp[1]);
      for (LongWritable value : values) {
        temp = value.get();
        System.out.println("Temp value is "+temp);
        temp=temp/denom;
        
      }
      context.write(new Text(tmp[0]), new LongWritable(temp));
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: wordFrequency <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(wordFrequency.class);
    job.setJobName("wordFrequency");
    job.setNumReduceTasks(3);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(FreqMap.class);
    job.setReducerClass(FreqReduce.class);
    job.setCombinerClass(Combiner.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
