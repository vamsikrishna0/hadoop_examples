package com.jet.hadoop.google.onegram.syllables;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Title: GoogleDataSyllablesToolImpl.java<br>
 * Description: <br>
 * Created: 11-Apr-2016<br>
 * Copyright: Copyright (c) 2015<br>
 * @author Teja Sasank Gorthi (gteja231989@gmail.com)
 */
public class GoogleDataSyllablesToolImpl extends Configured implements Tool {
    private static final int EXPECTED_ARGS = 3;

   public int run(String[] args) throws Exception {
         if(args.length != EXPECTED_ARGS) {
             System.err.printf("Invalid input and output file locations", getClass().getSimpleName());
             ToolRunner.printGenericCommandUsage(System.err);
             return -1;
        }
         @SuppressWarnings("deprecation")
         Job syllablesCounterJob = new Job();
         syllablesCounterJob.setJobName("GoogleDataSyllablesCounterJob");
         syllablesCounterJob.setJarByClass(GoogleDataSyllablesToolImpl.class);//Used for identifying the Jar containing the Mapper and Reducer classes.
         
         syllablesCounterJob.setMapperClass(GoogleDataSyllablesMapper.class);
         syllablesCounterJob.setReducerClass(GoogleDataSyllablesReducer.class);
         syllablesCounterJob.setCombinerClass(GoogleDataSyllablesReducer.class);

         FileInputFormat.addInputPath(syllablesCounterJob, new Path(args[1]));
         FileOutputFormat.setOutputPath(syllablesCounterJob, new Path(args[2]));

         syllablesCounterJob.setOutputKeyClass(Text.class);
         syllablesCounterJob.setOutputValueClass(IntWritable.class);
         syllablesCounterJob.setOutputFormatClass(TextOutputFormat.class);
         return syllablesCounterJob.waitForCompletion(true) ? 0 :1;
   }

}
