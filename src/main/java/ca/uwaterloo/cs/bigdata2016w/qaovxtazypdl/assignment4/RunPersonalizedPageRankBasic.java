package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment4;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.map.HMapIF;
import tl.lin.data.map.MapIF;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank.
 * </p>
 *
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 *
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);
  private static final String SOURCES_FIELD = "node.srcs";
  private static final String SOURCES_COUNT_FIELD = "node.srcs.count";

  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  // Mapper, no in-mapper combining.
  private static class MapClass extends
      Mapper<IntWritable, MultiSourcePageRankNode, IntWritable, MultiSourcePageRankNode> {

    // The neighbor to which we're sending messages.
    private static final IntWritable neighbor = new IntWritable();

    // Contents of the messages: partial PageRank mass.
    private static final MultiSourcePageRankNode intermediateMass = new MultiSourcePageRankNode();

    // For passing along node structure.
    private static final MultiSourcePageRankNode intermediateStructure = new MultiSourcePageRankNode();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      int srcCount = context.getConfiguration().getInt(SOURCES_COUNT_FIELD, 1);
      intermediateMass.setNumSourcesAndClear(srcCount);
      intermediateStructure.setNumSourcesAndClear(srcCount);
    }

    @Override
    public void map(IntWritable nid, MultiSourcePageRankNode node, Context context)
        throws IOException, InterruptedException {
      int srcCount = context.getConfiguration().getInt(SOURCES_COUNT_FIELD, 1);
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(MultiSourcePageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacenyList().size() > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        float massDiff = (float) StrictMath.log(list.size());
        ArrayListOfIntsWritable list = node.getAdjacenyList();

        context.getCounter(PageRank.edges).increment(list.size());

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.setNodeId(list.get(i));
          intermediateMass.setType(MultiSourcePageRankNode.Type.Mass);
          for (int i = 0; i < srcCount; i++) {
            intermediateMass.setPageRank(i, node.getPageRank(i) - massDiff);
          }

          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
    }
  }

  // Combiner: sums partial PageRank contributions and passes node structure along.
  private static class CombineClass extends
      Reducer<IntWritable, MultiSourcePageRankNode, IntWritable, MultiSourcePageRankNode> {
    private static final MultiSourcePageRankNode intermediateMass = new MultiSourcePageRankNode();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      int srcCount = context.getConfiguration().getInt(SOURCES_COUNT_FIELD, 1);
      intermediateMass.setNumSourcesAndClear(srcCount);
    }

    @Override
    public void reduce(IntWritable nid, Iterable<MultiSourcePageRankNode> values, Context context)
        throws IOException, InterruptedException {
      int massMessages = 0;
      int srcCount = context.getConfiguration().getInt(SOURCES_COUNT_FIELD, 1);

      // Remember, PageRank mass is stored as a log prob.
      float mass[srcCount] = new Float[srcCount];
      for (int i = 0; i < srcCount; i++) {
         mass[i] = Float.NEGATIVE_INFINITY;
      }

      for (MultiSourcePageRankNode n : values) {
        if (n.getType() == MultiSourcePageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } else {
          // Accumulate PageRank mass contributions.
          for (int i = 0; i < srcCount; i++) {
            mass[i] = sumLogProbs(mass[i], n.getPageRank(i));
          }
          massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(MultiSourcePageRankNode.Type.Mass);
        for (int i = 0; i < srcCount; i++) {
          intermediateMass.setPageRank(i, mass[i]);
        }
        context.write(nid, intermediateMass);
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends
      Reducer<IntWritable, MultiSourcePageRankNode, IntWritable, MultiSourcePageRankNode> {
    // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
    // through dangling nodes.
    private float totalMass = Float.NEGATIVE_INFINITY;

    @Override
    public void reduce(IntWritable nid, Iterable<MultiSourcePageRankNode> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<MultiSourcePageRankNode> values = iterable.iterator();

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      MultiSourcePageRankNode node = new MultiSourcePageRankNode();
      int srcCount = context.getConfiguration().getInt(SOURCES_COUNT_FIELD, 1);

      node.setType(MultiSourcePageRankNode.Type.Complete);
      node.setNodeId(nid.get());
      node.setNumSourcesAndClear(srcCount);

      int massMessagesReceived = 0;
      int structureReceived = 0;

      float mass[srcCount] = new Float[srcCount];
      for (int i = 0; i < srcCount; i++) {
         mass[i] = Float.NEGATIVE_INFINITY;
      }

      while (values.hasNext()) {
        MultiSourcePageRankNode n = values.next();

        if (n.getType().equals(MultiSourcePageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacenyList();
          structureReceived++;

          node.setAdjacencyList(list);
        } else {
          // This is a message that contains PageRank mass; accumulate.
          for (int i = 0; i < srcCount; i++) {
            mass[i] = sumLogProbs(mass[i], n.getPageRank(i));
          }
          massMessagesReceived++;
        }
      }

      // Update the final accumulated PageRank mass.
      for (int i = 0; i < srcCount; i++) {
        node.setPageRank(i, mass[i]);
      }
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node);

        // Keep track of total PageRank mass.
        totalMass = sumLogProbs(totalMass, mass);
      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
            + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // Write to a file the amount of PageRank mass we've seen in this reducer.
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      out.writeFloat(totalMass);
      out.close();
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private static class MapPageRankMassDistributionClass extends
      Mapper<IntWritable, MultiSourcePageRankNode, IntWritable, MultiSourcePageRankNode> {
    private float missingMass = 0.0f;
    private int nodeCnt = 0;
    private ArrayList<Long> sources = new ArrayList<Long>();

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      missingMass = conf.getFloat("MissingMass", 0.0f);
      nodeCnt = conf.getInt("NodeCount", 0);

      String srcStrings[] = conf.getStrings(SOURCES_FIELD);
      for (int i = 0; i < srcStrings.length; i++) {
        sources.add(Long.parseLong(srcStrings[i]));
      }
    }

    @Override
    public void map(IntWritable nid, MultiSourcePageRankNode node, Context context)
        throws IOException, InterruptedException {
      int srcCount = context.getConfiguration().getInt(SOURCES_COUNT_FIELD, 1);

      for (int i = 0; i < srcCount; i++) {
        float p = node.getPageRank(i);
        float jump, link;
        if (nid.get() == sources.get(i)) {
          jump = (float) Math.log(ALPHA);
          link = (float) Math.log(1.0f - ALPHA) + sumLogProbs(p, (float) Math.log(missingMass));
        } else {
          jump = (float) Math.log(0);
          link = (float) Math.log(1.0f - ALPHA) + p;
        }

        p = sumLogProbs(jump, link);
        node.setPageRank(i, p);
      }

      context.write(nid, node);
    }
  }

  // Random jump factor.
  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
  }

  public RunPersonalizedPageRankBasic() {}

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String SOURCES = "sources";
  private static final String START = "start";
  private static final String END = "end";
  private static final String COMBINER = "useCombiner";
  private static final String RANGE = "range";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(COMBINER, "use combiner"));
    options.addOption(new Option(RANGE, "use range partitioner"));

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("end iteration").create(END));
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

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
        !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String basePath = cmdline.getOptionValue(BASE);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    int s = Integer.parseInt(cmdline.getOptionValue(START));
    int e = Integer.parseInt(cmdline.getOptionValue(END));
    boolean useCombiner = cmdline.hasOption(COMBINER);
    boolean useRange = cmdline.hasOption(RANGE);
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: RunPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    LOG.info(" - use combiner: " + useCombiner);
    LOG.info(" - user range partitioner: " + useRange);
    LOG.info(" - sources: " + sources);

    getConf().setStrings(SOURCES_FIELD, sources);
    getConf().setInt(SOURCES_COUNT_FIELD, sources.split(",").length);

    // Iterate PageRank.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, useCombiner);
    }

    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int i, int j, String basePath, int numNodes,
      boolean useCombiner) throws Exception {
    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges.
    float mass = phase1(i, j, basePath, numNodes, useCombiner);

    // Find out how much PageRank mass got lost at the dangling nodes.
    float missing = 1.0f - (float) StrictMath.exp(mass);

    // Job 2: distribute missing mass, take care of random jump factor.
    phase2(i, j, missing, basePath, numNodes);
  }

  private float phase1(int i, int j, String basePath, int numNodes,
      boolean useCombiner) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(i);
    String out = basePath + "/iter" + formatter.format(j) + "t";
    String outm = out + "-mass";

    // We need to actually count the number of part files to get the number of partitions (because
    // the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + j + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);
    LOG.info(" - useCombiner: " + useCombiner);
    LOG.info("computed number of partitions: " + numPartitions);

    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
    job.getConfiguration().set("PageRankMassPath", outm);

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MultiSourcePageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(MultiSourcePageRankNode.class);

    job.setMapperClass(MapClass.class);

    if (useCombiner) {
      job.setCombinerClass(CombineClass.class);
    }

    job.setReducerClass(ReduceClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    float mass = Float.NEGATIVE_INFINITY;
    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());
      mass = sumLogProbs(mass, fin.readFloat());
      fin.close();
    }

    return mass;
  }

  private void phase2(int i, int j, float missing, String basePath, int numNodes) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    LOG.info("missing PageRank mass: " + missing);
    LOG.info("number of nodes: " + numNodes);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);

    LOG.info("PageRank: iteration " + j + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    job.getConfiguration().setFloat("MissingMass", (float) missing);
    job.getConfiguration().setInt("NodeCount", numNodes);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MultiSourcePageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(MultiSourcePageRankNode.class);

    job.setMapperClass(MapPageRankMassDistributionClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }

  // Adds two log probs.
  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }

    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
}
