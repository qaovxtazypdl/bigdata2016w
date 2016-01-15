package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment1;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStrings;

/**
 * Simple word count demo.
 */
public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
  private static final String sideDataOutput = "PairsPMISideData";

  // Mapper: emits (token, 1) for every word occurrence.
  private static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);

      int cnt = 0;
      HashSet<String> set = new HashSet<String>();
      while (itr.hasMoreTokens()) {
        cnt++;
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        set.add(w);
        if (cnt >= 100) break;
      }

      for (String word : set) {
        WORD.set(word);
        context.write(WORD, ONE);
      }

      WORD.set("*");
      context.write(WORD, ONE);
    }
  }

  // Reducer: sums up all the counts.
  private static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);

      if (sum >= 10) {
        context.write(key, SUM);
      }
    }
  }





  // Mapper: emits (token, 1) for every word occurrence.
  private static class PMIMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private final static IntWritable ONE = new IntWritable(1);
    private final static PairOfStrings KEY = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);

      int cnt = 0;
      HashSet<String> set = new HashSet<String>();
      while (itr.hasMoreTokens()) {
        cnt++;
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        set.add(w);
        if (cnt >= 100) break;
      }

      String[] words = new String[set.size()];
      words = set.toArray(words);

      for (int i = 0; i < words.length; i++) {
        for (int j = 0; j < words.length; j++) {
          if (i == j) continue;

          KEY.set(words[i], words[j]);
          context.write(KEY, ONE);
        }
      }
    }
  }

  private static class PMICombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  // Reducer: sums up all the counts.
  private static class PMIReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, FloatWritable>  {
    private HashMap<String, Integer> countMap = new HashMap<String, Integer>();
    private float pX = 0, pY = 0, pXY = 0;
    private static final FloatWritable PMI = new FloatWritable();

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      Path sideDataDir = new Path(sideDataOutput);

      try {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(sideDataDir);

        for (int i = 0;i < status.length; i++) {
          if (!status[i].getPath().toString().contains("part-") || status[i].getPath().toString().contains(".crc")) continue;
          BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
          String line;
          while ((line = br.readLine()) != null) {
            String[] lineTokens = line.split("\t");
            countMap.put(lineTokens[0], Integer.parseInt(lineTokens[1]));
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int coOccurrenceTimes = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        coOccurrenceTimes += iter.next().get();
      }

      try {
        if (coOccurrenceTimes >= 10) {
          int totalLines = countMap.get("*");
          int totalKeyCount = countMap.get(key.getKey());
          int totalPairSecondOccurrenceTimes = countMap.get(key.getValue());
          pX = (float)totalKeyCount / totalLines;
          pY = (float)totalPairSecondOccurrenceTimes / totalLines;
          pXY = (float)coOccurrenceTimes / totalLines;

          PMI.set((float) Math.log10(pXY / (pX * pY)));
          context.write(key, PMI);
        }
      } catch (Exception e) {
        LOG.error("" + e.getMessage());
        LOG.error("Failed on word " + key.getKey() + " " + key.getValue());
        LOG.error("More info: " + countMap.get(key.getKey()) + " " + countMap.get(key.getValue()));
      }
    }
  }


  /**
   * Creates an instance of this tool.
   */
  public PairsPMI() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Configuration conf = getConf();
    Job job1 = Job.getInstance(conf);
    job1.setJobName(PairsPMI.class.getSimpleName());
    job1.setJarByClass(PairsPMI.class);

    job1.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(sideDataOutput));

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setMapperClass(CountMapper.class);
    job1.setCombinerClass(CountReducer.class);
    job1.setReducerClass(CountReducer.class);

    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");


    Job job2 = Job.getInstance(conf);
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(FloatWritable.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(PMIMapper.class);
    job2.setCombinerClass(PMICombiner.class);
    job2.setReducerClass(PMIReducer.class);

    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directories if it exists already.
    Path outputDir = new Path(args.output);
    Path sideDataDir = new Path(sideDataOutput);
    FileSystem.get(conf).delete(outputDir, true);
    FileSystem.get(conf).delete(sideDataDir, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    job2.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
