import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TwitterFollowerCounterGroupByRanges {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private IntWritable key = new IntWritable();
    private final static IntWritable one = new IntWritable(1);

    private int makeRangeKeyFromCountFollowers(int count_followers) {  // 22 -> 100
      int x = 1;
      while (x <= count_followers) {
        x *= 10;
      }
      return x;
    }

    public void map(LongWritable _key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] twitter_id_and_count_followers = line.split("\t");
      if (twitter_id_and_count_followers.length == 2) {
        Integer _count_followers = Integer.parseInt(twitter_id_and_count_followers[1]);
        key.set(makeRangeKeyFromCountFollowers(_count_followers));
        output.collect(key, one);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable val = new IntWritable();

    public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(TwitterFollowerCounterGroupByRanges.class);
    conf.setJobName("TwitterFollowerCounterGroupByRanges");

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(IntWritable.class);

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
