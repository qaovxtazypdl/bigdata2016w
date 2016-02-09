package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment4;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.pair.PairOfIntFloat;
import tl.lin.data.queue.TopScoredObjects;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends
      Mapper<IntWritable, MultiSourcePageRankNode, IntWritable, PairOfIntFloat> {
    private TopScoredObjects<Integer>[] queues;

    @Override
    public void setup(Context context) throws IOException {
      int srcCount = context.getConfiguration().getInt("srcCount", 1);
      int k = context.getConfiguration().getInt("n", 100);
      queues = new TopScoredObjects[srcCount];
      for (int i = 0; i < srcCount; i++) {
        queues[i] = new TopScoredObjects<Integer>(k);
      }
    }

    @Override
    public void map(IntWritable nid, MultiSourcePageRankNode node, Context context) throws IOException,
        InterruptedException {
      int srcCount = context.getConfiguration().getInt("srcCount", 1);
      for (int i = 0; i < srcCount; i++) {
        queues[i].add(node.getNodeId(), node.getPageRank(i));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      int srcCount = context.getConfiguration().getInt("srcCount", 1);
      PairOfIntFloat value = new PairOfIntFloat();

      for (int i = 0; i < srcCount; i++) {
        for (PairOfObjectFloat<Integer> pair : queues[i].extractAll()) {
          key.set(pair.getLeftElement());
          value.set(i, pair.getRightElement());
          context.write(key, value);
        }
      }
    }
  }

  private static class MyReducer extends
      Reducer<IntWritable, PairOfIntFloat, LongWritable, Text> {
    private TopScoredObjects<Integer>[] queues;
    private ArrayList<Long> sources = new ArrayList<Long>();

    @Override
    public void setup(Context context) throws IOException {
      int srcCount = context.getConfiguration().getInt("srcCount", 1);
      String srcStrings[] = context.getConfiguration().getStrings("sources");
      for (int i = 0; i < srcCount; i++) {
        sources.add(Long.parseLong(srcStrings[i]));
      }

      int k = context.getConfiguration().getInt("n", 100);
      queues = new TopScoredObjects[srcCount];
      for (int i = 0; i < srcCount; i++) {
        queues[i] = new TopScoredObjects<Integer>(k);
      }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PairOfIntFloat> iterable, Context context)
        throws IOException {
      Iterator<PairOfIntFloat> iter = iterable.iterator();
      while (iter.hasNext()) {
        PairOfIntFloat next = iter.next();
        int index = next.getLeftElement();
        queues[index].add(nid.get(), next.getRightElement());
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      LongWritable key = new LongWritable();
      Text value = new Text();
      int srcCount = context.getConfiguration().getInt("srcCount", 1);

      for (int i = 0; i < srcCount; i++) {
        key.set(sources.get(i));
        for (PairOfObjectFloat<Integer> pair : queues[i].extractAll()) {
          value.set(String.format("%.5f %d", (float) StrictMath.exp(pair.getRightElement()), pair.getLeftElement()));
          context.write(key, value);
        }
      }
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String SOURCES = "sources";
  private static final String TOP = "top";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sources = cmdline.getOptionValue(SOURCES);
    int srcCount = sources.split(",").length;

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sources: " + sources);



    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.setInt("srcCount", srcCount);
    conf.setStrings("sources", sources);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PairOfIntFloat.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    String srcStrings[] = sources.split(",");

    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outputPath + "/part-r-00000"))) {
      FSDataInputStream fin = fs.open(f.getPath());
      for (int s = 0; s < srcCount; s++) {
        System.out.println("Source: " + srcStrings[s]);
        for (int i = 0; i < n; i++) {
          String r = fin.readLine();
          System.out.println(r.substring(r.indexOf("\t") + 1));
        }

        if (s != srcCount - 1) {
          System.out.println();
        }
      }
      fin.close();
    }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
