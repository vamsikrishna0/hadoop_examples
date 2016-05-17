package com.jet.hadoop.google.onegram.syllables;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.jet.hadoop.google.onegram.helper.Syllable;


/**
 * Title: GoogleDataSyllablesMapper.java<br>
 * Description: <br>
 * Created: 11-Apr-2016<br>
 * Copyright: Copyright (c) 2015<br>
 * @author Teja Sasank Gorthi (gteja231989@gmail.com)
 */
public class GoogleDataSyllablesMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
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
          String word  = array[0].trim();
          int number_of_syllables = Syllable.syllable(word);
          context.write(year, new IntWritable(number_of_syllables));
    }
}
