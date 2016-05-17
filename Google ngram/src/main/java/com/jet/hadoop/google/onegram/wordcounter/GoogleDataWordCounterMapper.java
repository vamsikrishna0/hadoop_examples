package com.jet.hadoop.google.onegram.wordcounter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Title: GoogleDataWordCounterMapper.java<br>
 * Description: <br>
 * Created: 11-Apr-2016<br>
 * Copyright: Copyright (c) 2015<br>
 * @author Teja Sasank Gorthi (gteja231989@gmail.com)
 */
public class GoogleDataWordCounterMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

   private static final IntWritable ONE = new IntWritable(1);
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
         context.write(year, ONE);
   }
}
