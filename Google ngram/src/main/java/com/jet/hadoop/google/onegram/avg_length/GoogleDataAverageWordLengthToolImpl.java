package com.jet.hadoop.google.onegram.avg_length;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GoogleDataAverageWordLengthToolImpl extends Configured implements Tool {
     private static final int EXPECTED_ARGS = 3;

   public int run(String[] args) throws Exception {
         if(args.length != EXPECTED_ARGS) {
             System.err.printf("Invalid input and output file locations", getClass().getSimpleName());
             ToolRunner.printGenericCommandUsage(System.err);
             return -1;
        }

         @SuppressWarnings("deprecation")
         Job avgWordLengthJob = new Job();

         avgWordLengthJob.setJobName("GoogleDataAverageWordLengthFinderJob");
         avgWordLengthJob.setJarByClass(GoogleDataAverageWordLengthToolImpl.class);

         avgWordLengthJob.setMapperClass(GoogleDataAverageWordLengthMapper.class);
         avgWordLengthJob.setReducerClass(GoogleDataAverageWordLengthReducer.class);
         avgWordLengthJob.setCombinerClass(GoogleDataAverageWordLengthReducer.class);


         FileInputFormat.addInputPath(avgWordLengthJob, new Path(args[1]));
         FileOutputFormat.setOutputPath(avgWordLengthJob, new Path(args[2]));

         avgWordLengthJob.setOutputKeyClass(Text.class);
         avgWordLengthJob.setOutputValueClass(FloatWritable.class);
         avgWordLengthJob.setOutputFormatClass(TextOutputFormat.class);

         return avgWordLengthJob.waitForCompletion(true)? 0:1;
   }

}
