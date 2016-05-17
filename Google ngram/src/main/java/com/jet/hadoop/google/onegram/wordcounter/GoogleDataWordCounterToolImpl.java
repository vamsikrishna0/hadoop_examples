package com.jet.hadoop.google.onegram.wordcounter;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Title: GoogleDataWordCounterToolImpl.java<br>
 * Description: <br>
 * Created: 11-Apr-2016<br>
 * Copyright: Copyright (c) 2015<br>
 * @author Teja Sasank Gorthi (gteja231989@gmail.com)
 */
public class GoogleDataWordCounterToolImpl extends Configured implements Tool {
  private static final int EXPECTED_ARGS = 3;

   public int run(String[] args) throws Exception {
      if(args.length != EXPECTED_ARGS) {
          System.err.printf("Invalid input and output file locations", getClass().getSimpleName());
          ToolRunner.printGenericCommandUsage(System.err);
          return -1;
     }

      @SuppressWarnings("deprecation")
      Job wordCounterJob = new Job();
      wordCounterJob.setJobName("GoogleDataWordCounterJob");
      wordCounterJob.setJarByClass(GoogleDataWordCounterToolImpl.class);//Used for identifying the Jar containing the Mapper and Reducer classes.
      
      wordCounterJob.setMapperClass(GoogleDataWordCounterMapper.class);
      wordCounterJob.setReducerClass(GoogleDataWordCounterReducer.class);
      wordCounterJob.setCombinerClass(GoogleDataWordCounterReducer.class);

      FileInputFormat.addInputPath(wordCounterJob, new Path(args[1]));
      FileOutputFormat.setOutputPath(wordCounterJob, new Path(args[2]));

      wordCounterJob.setOutputKeyClass(Text.class);
      wordCounterJob.setOutputValueClass(IntWritable.class);
      wordCounterJob.setOutputFormatClass(TextOutputFormat.class);
      return wordCounterJob.waitForCompletion(true) ? 0 :1;
   }

}
