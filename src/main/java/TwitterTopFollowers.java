import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TwitterTopFollowers {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
    private final static IntWritable static_key = new IntWritable(0);
    private Text val = new Text();

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] twitter_id_and_count_followers = line.split("\t");
      if (twitter_id_and_count_followers.length == 2) {
        val.set(line);
        output.collect(static_key, val);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
    private Text val = new Text();

    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      java.util.Map<Integer, String> count_to_line = new TreeMap<Integer, String>(new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return -1*o1.compareTo(o2);
        }
      });
      while (values.hasNext()) {
        String line = values.next().toString();
        String[] twitter_id_and_count_followers = line.split("\t");
        Integer _count_followers = Integer.parseInt(twitter_id_and_count_followers[1]);
        count_to_line.put(_count_followers, line);
      }
      
      int i = 0;
      for(java.util.Map.Entry<Integer, String> e : count_to_line.entrySet()){
          if (i++ >= 50) break;
          val.set(e.getValue());
          output.collect(key, val);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(TwitterTopFollowers.class);
    conf.setJobName("TwitterTopFollowers");

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
