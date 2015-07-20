import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.path;
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

public class WordCount2 {
   public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    
      static enum CounterEnum { INPUT_WORDS }
      
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      private boolean caseSensitive;
      private Set<String> patternsToSkip = new HashSet<String>();

      private Configuration conf;
      private BufferedReader fis;

      @Override
      public void setup(Context context) throws IOException,InterruptedException {
        conf = context.getConfiguration();
        caseSentive = conf.getBoolean("wordcount.case.sensitive", true);
        if ( conf.getBoolean("wordcount.skip.patterns", true)) {
          URI[] patternsURIs = Job.getInstane(conf).getCacheFiles();
          for (URI patternsURI : patternsURIs) {
            Path patternsPath = new Path(patternsURI.getPath());
            String patternsFileName = patternsPath.getName.toString();
            parseSkipFile(patternsFileName);
          }
        }
      }

     private parseSkipFile(String fileName) {
       try {
         fis = new BufferedReader(new FileReader(fileName));
         String pattern = null;
         while ((pattern = fis.readLine() != null) {
           patternsToSkip.add(pattern);
         }

       } catch (IIOExcption ioe) {
	 System.err.println("Caught exception while parsing the cached file " + StringUtils.stringifyException(ioe));
       }


    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
      String line = (caseSensitive) ? value.toString : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, "");
      }
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
        Counter counter = context.getCounter(CountersEnum.class.getName(),
            countersEnum.INPUT_WORDS.toString());
        counter.increment(1);
      }
    }

} 





