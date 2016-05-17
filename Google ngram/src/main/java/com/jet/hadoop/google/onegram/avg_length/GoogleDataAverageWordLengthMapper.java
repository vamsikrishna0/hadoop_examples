package com.jet.hadoop.google.onegram.avg_length;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GoogleDataAverageWordLengthMapper extends Mapper<LongWritable, Text,Text, FloatWritable> {
    private static final String DELIMITER = "\t";

    /*Data Format
      X'rays   1948   2   2
      X'rays   1952   25   2
   */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String[] array = value.toString().split(DELIMITER);
          Text year = new Text();
          year.set(array[1].trim());
          int word_length = array[0].trim().length();
          context.write(year, new FloatWritable(word_length));
    }
}
