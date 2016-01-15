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
import tl.lin.data.map.HMapStIW;

/**
 * Simple word count demo.
 */
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  private static final String sideDataOutput = "StripesPMISideData";

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
  private static class CountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
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

      context.write(key, SUM);
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
  private static class PMIMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    // Reuse objects to save overhead of object creation.
    private final static Text WORD = new Text();
    private static final HMapStIW MAP = new HMapStIW();

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

          MAP.clear();
          MAP.put(words[j], 1);
          WORD.set(words[i]);
          context.write(WORD, MAP);
        }
      }
    }
  }

  private static class PMICombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  // Reducer: sums up all the counts.
  private static class PMIReducer extends Reducer<Text, HMapStIW, PairOfStrings, FloatWritable>  {
    private HashMap<String, Integer> countMap = new HashMap<String, Integer>();
    private float pX = 0, pY = 0, pXY = 0;
    private static final FloatWritable PMI = new FloatWritable();
    private static final PairOfStrings PMI_PAIR = new PairOfStrings();

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      Path sideDataDir = new Path(sideDataOutput);

      FileSystem fs = FileSystem.get(conf);
      FileStatus[] status = fs.listStatus(sideDataDir);

      for (int i = 0;i < status.length; i++) {
        if (!status[i].getPath().toString().contains("part-") || status[i].getPath().toString().contains(".crc")) continue;
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
        String line;
        while ((line = br.readLine()) != null) {
          String[] lineTokens = line.split("\\s+");
          LOG.error("@@@@@@@@@@@@@@@@@@@@@@ " + new Text(lineTokens[0].getBytes("UTF-8")).toString() +  " written to map with value " + Integer.parseInt(lineTokens[1]));
          countMap.put(new Text(lineTokens[0].getBytes("UTF-8")).toString(), Integer.parseInt(lineTokens[1]));
        }
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      int totalLines = countMap.get("*");

      for (String pairSecond : map.keySet()) {
        try {
          int coOccurrenceTimes = map.get(pairSecond);
          if (coOccurrenceTimes >= 10) {
            int totalPairSecondOccurrenceTimes = countMap.get(pairSecond);
            int totalKeyCount = countMap.get(key.toString());
            pX = (float)totalKeyCount / totalLines;
            pY = (float)totalPairSecondOccurrenceTimes / totalLines;
            pXY = (float)coOccurrenceTimes / totalLines;

            PMI.set((float) Math.log10(pXY / (pX * pY)));
            PMI_PAIR.set(key.toString(), pairSecond);
            context.write(PMI_PAIR, PMI);
          }
        } catch (Exception e) {
          LOG.error(e.toString());
          LOG.error("Failed on word " + key + " " + key.toString() + " " + pairSecond);
          LOG.error("More info: " + countMap.get(key.toString()) + " " + countMap.get(pairSecond));
        }
      }
    }
  }


  /**
   * Creates an instance of this tool.
   */
  public StripesPMI() {}

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

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Configuration conf = getConf();
    Job job1 = Job.getInstance(conf);
    job1.setJobName(StripesPMI.class.getSimpleName());
    job1.setJarByClass(StripesPMI.class);

    job1.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(sideDataOutput));

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setMapperClass(CountMapper.class);
    job1.setCombinerClass(CountCombiner.class);
    job1.setReducerClass(CountReducer.class);

    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m -Dfile.encoding=UTF-8");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");


    Job job2 = Job.getInstance(conf);
    job2.setJobName(StripesPMI.class.getSimpleName());
    job2.setJarByClass(StripesPMI.class);

    job2.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job2, new Path(args.input));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(HMapStIW.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(FloatWritable.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(PMIMapper.class);
    job2.setCombinerClass(PMICombiner.class);
    job2.setReducerClass(PMIReducer.class);

    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m -Dfile.encoding=UTF-8");
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
    ToolRunner.run(new StripesPMI(), args);
  }
}
