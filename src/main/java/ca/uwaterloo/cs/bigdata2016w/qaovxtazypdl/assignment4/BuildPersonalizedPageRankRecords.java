package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment4;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfIntsWritable;

/**
 * <p>
 * Driver program that takes a plain-text encoding of a directed graph and builds corresponding
 * Hadoop structures for representing the graph.
 * </p>
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class BuildPersonalizedPageRankRecords extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildPersonalizedPageRankRecords.class);

  private static final String SOURCES_FIELD = "node.srcs";

  private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, MultiSourcePageRankNode> {
    private static final IntWritable nid = new IntWritable();
    private static final MultiSourcePageRankNode node = new MultiSourcePageRankNode();
    private ArrayList<Long> sources = new ArrayList<Long>();

    public void setup(Mapper<LongWritable, Text, IntWritable, MultiSourcePageRankNode>.Context context) {
      String srcStrings[] = context.getConfiguration().getStrings(SOURCES_FIELD);
      for (int i = 0; i < srcStrings.length; i++) {
        sources.add(Long.parseLong(srcStrings[i]));
      }
      node.setType(MultiSourcePageRankNode.Type.Complete);
      node.setNumSourcesAndClear(srcStrings.length);
    }

    @Override
    public void map(LongWritable key, Text t, Context context) throws IOException,
        InterruptedException {

      for (int i = 0; i < sources.size(); i++) {
        node.setPageRank(i, (float) StrictMath.log(key.get() == sources.get(i)?1:0));
      }

      String[] arr = t.toString().trim().split("\\s+");

      nid.set(Integer.parseInt(arr[0]));
      if (arr.length == 1) {
        node.setNodeId(Integer.parseInt(arr[0]));
        node.setAdjacencyList(new ArrayListOfIntsWritable());

      } else {
        node.setNodeId(Integer.parseInt(arr[0]));

        int[] neighbors = new int[arr.length - 1];
        for (int i = 1; i < arr.length; i++) {
          neighbors[i - 1] = Integer.parseInt(arr[i]);
        }

        node.setAdjacencyList(new ArrayListOfIntsWritable(neighbors));
      }

      context.getCounter("graph", "numNodes").increment(1);
      context.getCounter("graph", "numEdges").increment(arr.length - 1);

      if (arr.length > 1) {
        context.getCounter("graph", "numActiveNodes").increment(1);
      }

      context.write(nid, node);
    }
  }

  public BuildPersonalizedPageRankRecords() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String SOURCES = "sources";
  private static final String NUM_NODES = "numNodes";

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
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(NUM_NODES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    String sources = cmdline.getOptionValue(SOURCES);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));

    LOG.info("Tool name: " + BuildPersonalizedPageRankRecords.class.getSimpleName());
    LOG.info(" - inputDir: " + inputPath);
    LOG.info(" - outputDir: " + outputPath);
    LOG.info(" - sources: " + sources);
    LOG.info(" - numNodes: " + n);

    Configuration conf = getConf();
    conf.setStrings(SOURCES_FIELD, sources);
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);

    Job job = Job.getInstance(conf);
    job.setJobName(BuildPersonalizedPageRankRecords.class.getSimpleName() + ":" + inputPath);
    job.setJarByClass(BuildPersonalizedPageRankRecords.class);

    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MultiSourcePageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(MultiSourcePageRankNode.class);

    job.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildPersonalizedPageRankRecords(), args);
  }
}
