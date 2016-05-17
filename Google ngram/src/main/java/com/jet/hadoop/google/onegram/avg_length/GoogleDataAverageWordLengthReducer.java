package com.jet.hadoop.google.onegram.avg_length;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GoogleDataAverageWordLengthReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

   @Override
   public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
         int total_words_used = 0;
         float total_length = 0;
         Iterator<FloatWritable> itr = values.iterator();
         while(itr.hasNext()) {
            total_length = total_length + itr.next().get();
            total_words_used++;
         }
         float average_length = total_length/total_words_used;
         context.write(key, new FloatWritable(average_length));
      }
}
