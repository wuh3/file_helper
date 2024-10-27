import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private static final HashSet<Character> REAL_WORD = new HashSet<>(Arrays.asList('m', 'M', 'n', 'N', 'o', 'O', 'p', 'P', 'q','Q'));
    private HashMap<String, Integer> counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.counter = new HashMap<>();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry: counter.entrySet()) {
            word.set(entry.getKey());
            IntWritable val = new IntWritable(entry.getValue());
            context.write(word, val);
        }
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String next = itr.nextToken();
            if (REAL_WORD.contains(next.charAt(0))) {
                counter.put(next, counter.getOrDefault(next, 0) + 1);
            }
        }

    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

    public static class RealWordPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            char firstLetter = key.toString().toLowerCase().charAt(0);
            switch (firstLetter) {
                case 'm':
                    return 0 % numPartitions;
                case 'n':
                    return 1 % numPartitions;
                case 'o':
                    return 2 % numPartitions;
                case 'p':
                    return 3 % numPartitions;
                case 'q':
                    return 4 % numPartitions;
                default:
                    return 0;
            }
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setPartitionerClass(RealWordPartitioner.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}