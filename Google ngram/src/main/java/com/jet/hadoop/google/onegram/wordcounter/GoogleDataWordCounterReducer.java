package com.jet.hadoop.google.onegram.wordcounter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Title: GoogleDataWordCounterReducer.java<br>
 * Description: <br>
 * Created: 11-Apr-2016<br>
 * Copyright: Copyright (c) 2015<br>
 * @author Teja Sasank Gorthi (gteja231989@gmail.com)
 */
public class GoogleDataWordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

   @Override
   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         int total_words_used = 0;
         Iterator<IntWritable> itr = values.iterator();
         while(itr.hasNext()) {
            total_words_used = total_words_used + itr.next().get();  
         }
         context.write(key, new IntWritable(total_words_used));
      }
}
