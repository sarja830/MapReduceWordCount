package main;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WordCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    static enum CountersEnum {INPUT_WORDS}

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();
    private Set<String> stopWords = new HashSet<String>();
    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
      if (conf.getBoolean("wordcount.skip.patterns", true)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
//        for (URI patternsURI : patternsURIs) {
        Path patternsPath = new Path(patternsURIs[0].getPath());
//        System.out.println("skipwords "+patternsPath);
        String patternsFileName = patternsPath.getName().toString();
        parseSkipFile(patternsFileName);
      }

      if (conf.getBoolean("wordcount.skip.stopwords", true)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
//        for (URI patternsURI : patternsURIs) {
        Path patternsPath = new Path(patternsURIs[1].getPath());
//        System.out.println("skip   stop words"+ patternsPath);
        String patternsFileName = patternsPath.getName().toString();
        parseStopWordFile(patternsFileName);
      }
    }




    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
//          System.out.println("content "+pattern);
        }
      }
      catch (IOException ioe)
      {
        System.err.println("Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
      }
    }
    private void parseStopWordFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          stopWords.add(pattern);
//          System.out.println("content "+pattern);

        }
      }
      catch (IOException ioe)
      {
        System.err.println("Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
      }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = (caseSensitive) ?
              value.toString() : value.toString().toLowerCase();
//      for (String pattern : patternsToSkip) {
        line = line.replaceAll("[^a-zA-Z'\\s]+", " ");
        line = line.replaceAll("\\B'\\b|\\b'\\B", "");
//      }
      String[] result = line.split("\\s");
      for (int i=0; i<result.length; i++)
      {
        if (stopWords.contains(result[i].toLowerCase()))
          continue;
        else {
          word.set(result[i]);
          context.write(word, one);
          Counter counter = context.getCounter(CountersEnum.class.getName(),
                  CountersEnum.INPUT_WORDS.toString());
          counter.increment(1);
        }
      }
    }
  }

  public static class IntSumReducer
          extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
//    if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
//      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
//      System.exit(2);
//    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
//      System.out.print(remainingArgs[i]);
      if ("-skip".equals(remainingArgs[i]))
      {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      }
      else if ("-skipStopWords".equals(remainingArgs[i]))
      {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
//        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      }
      else
      {
        otherArgs.add(remainingArgs[i]);
      }
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
