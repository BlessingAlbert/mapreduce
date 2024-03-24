import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GGPlot2DownloadCounter {

  public static class GGPlot2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text packageName = new Text("ggplot2");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Assuming CSV structure: date,time,size,r_version,r_arch,r_os,package,version,country,ip_id
      String[] parts = value.toString().split(",");
      // Check if parts length is correct and package name matches "ggplot2"
      if (parts.length > 7 && parts[6].trim().equalsIgnoreCase("\"ggplot2\"")) {
        context.write(packageName, one); // Emit package name with count of 1
      }
    }
  }

  public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum)); // Emit total count for package
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: GGPlot2DownloadCounter <input path> <output path>");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "GGPlot2 Download Counter");
    job.setJarByClass(GGPlot2DownloadCounter.class);
    job.setMapperClass(GGPlot2Mapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
