import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
    
    private Text word = new Text();
    private Text docId;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] parts = line.split("\t", 2);
      docId = new Text(parts[0]);
      StringTokenizer itr = new StringTokenizer(parts[1]);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, docId);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String s = "";
      Hashtable<String, Integer> hm = new Hashtable<String, Integer>();
      for(Text val: values){
        String docId = val.toString();
        int count = hm.getOrDefault(docId, 0);
        hm.put(docId, count + 1);
      }
      Iterator ite = hm.entrySet().iterator();
      while(ite.hasNext()){
        Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>)ite.next();
        s = s + pair.getKey() + ":" + Integer.toString(pair.getValue()) + "\t";
      }
      Text result = new Text(s);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    // Configuration conf = new Configuration();
    Job job = new Job();
    //creating a hadoop job and assign a job name for identification
    job.setJarByClass(InvertedIndex.class);
    job.setJobName("Inverted Index");
    //providing the mapper and reducer class names
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //the HDFS input and output directories to be fetched from the Dataproc job submission console
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}