package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment3;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfWritables;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    private static final Text WORD = new Text();
    private static final IntWritable CNT = new IntWritable();
    private static final IntWritable DOCNO = new IntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();
    private static final PairOfStringInt KEY = new PairOfStringInt();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();

      // Tokenize line.
      List<String> tokens = new ArrayList<String>();
      StringTokenizer itr = new StringTokenizer(text);
      while (itr.hasMoreTokens()) {
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        tokens.add(w);
      }

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        KEY.set(e.getLeftElement(), (int) docno.get());
        CNT.set(e.getRightElement());
        context.write(KEY, CNT);
      }
    }
  }

  private static class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private static class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, PairOfWritables<IntWritable, BytesWritable>> {
    private final static Text term = new Text();
    private final static IntWritable DF = new IntWritable();
    private final static VIntWritable vint = new VIntWritable();
    private final static BytesWritable postingList = new BytesWritable();
    private final static ByteArrayOutputStream postingByteStream = new ByteArrayOutputStream();
    private final static DataOutputStream postingStream = new DataOutputStream(postingByteStream);

    private static String prevKey = null;
    private static int df = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      prevKey = null;
      postingByteStream.reset();
      df = 0;
    }

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // key is term,docid    values are counts
      // output key, <sizeoflist, listof(docid, count)>
      String termString = key.getLeftElement();
      int docid = key.getRightElement();
      if (prevKey != null && !prevKey.equals(termString)) {
        //emit previous, reset
        DF.set(df);
        postingByteStream.flush();
        postingList.set(postingByteStream.toByteArray(), 0, postingByteStream.size());
        term.set(prevKey);
        context.write(term, new PairOfWritables<IntWritable, BytesWritable>(DF, postingList));
        postingByteStream.reset();
        df = 0;
      }

      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        df += 1;
        WritableUtils.writeVInt(postingStream, docid);
        WritableUtils.writeVInt(postingStream, iter.next().get());
      }

      prevKey = termString;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      if (prevKey != null) {
        DF.set(df);
        term.set(prevKey);
        postingByteStream.flush();
        postingList.set(postingByteStream.toByteArray(), 0, postingByteStream.size());
        context.write(term, new PairOfWritables<IntWritable, BytesWritable>(DF, postingList));
      }
    }
  }

  private BuildInvertedIndexCompressed() {}

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

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
