package com.stat;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TermFrequency extends Configured implements Tool{
	
	public static class Map extends Mapper<Text,Text,Text,Text>
	{
		HashMap<String,Integer> words = new HashMap();
		HashMap<Character,Integer> letters = new HashMap();
		public void map (Text key, Text value, Context context)throws IOException,InterruptedException
		{
			//System.out.println("Inside Module 3 mapper: Value Received->"+value.toString());
			String str = value.toString();
			//System.out.println("Line is:"+line);
			Pattern pat = Pattern.compile("\\w+");
			//matches only words. Special characters will be ignored
			Matcher m = pat.matcher(str);
			double N = 0;
			while(m.find())
			{
				String word = m.group().toLowerCase();
				word = word.replaceAll("^0*", "");
				if(word.length()<2)
					continue;
				if(words.containsKey(word))
					words.put(word, words.get(word)+1);
				else
					words.put(word, 1);
				N = N+1;
			}
			Comparator<String> comparator1 = new ValueComparatorDesc(words);
			Comparator<String> comparator2 = new ValueComparatorAsc(words);
			TreeMap<String,Integer> TopTen = new TreeMap<String,Integer>(comparator1);
			TopTen.putAll(words);
			//System.out.println("Size of TopTen Map:"+TopTen.size());
			TreeMap<String,Integer> BottomTen = new TreeMap<String,Integer>(comparator2);
			BottomTen.putAll(words);
			//System.out.println("Size of Bottom Map:"+BottomTen.size());
			
			Iterator<Entry<String,Integer>> iter1 = TopTen.entrySet().iterator();
			Iterator<Entry<String,Integer>> iter2 = BottomTen.entrySet().iterator();
			Entry<String,Integer> entry = null;
			while(TopTen.size()>10)
			{
				entry = iter1.next();
				//Removing all the entries from the Tree Map except the top ten highest words
				iter1.remove();
				//System.out.println("Deleted Successfully");
			}
			while(BottomTen.size()>10)
			{
				entry = iter2.next();
				//Removing all the entries from the Tree Map except the top ten highest words
				iter2.remove();
				//System.out.println("Deleted Successfully");
			}
			//System.out.println("Size of TopTen Map after delete"+TopTen.size());
			int count = 0;
			//Highest ten words wrt count
			while(iter1.hasNext() && count<10)
			{
				Entry<String,Integer> temp = iter1.next();
				String word = temp.getKey();
				int n = temp.getValue();
				double percent = (n/N)*100;
				String new_key = key.toString()+","+word;
				String new_val = "Lowest Count---->"+n+","+N+","+percent;
				count++;
				context.write(new Text(new_key), new Text(new_val));
			}
			//System.out.println("Size of Bottom Map after delete"+BottomTen.size());
			count = 0;
			//Bottom Ten words wrt count
			while(iter2.hasNext() && count<10)
			{
				Entry<String,Integer> temp = iter2.next();
				String word = temp.getKey();
				int n = temp.getValue();
				double percent = (n/N)*100;
				String new_key = key.toString()+","+word;
				String new_val = "HighestCount---->"+n+","+N+","+percent;
				count++;
				context.write(new Text(new_key), new Text(new_val));
			}
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Text value,Context context)throws IOException,InterruptedException
		{
			context.write(key, value);
		}
	}

	public int run(String[] args) throws Exception {
		System.out.println("Inside Module 2:Input->"+args[0]+" Output->"+args[1]);
		Configuration conf = new Configuration();
		Job myjob = new Job(conf);
		myjob.setMapperClass(Map.class);
		myjob.setReducerClass(Reduce.class);
		myjob.setJarByClass(TermFrequency.class);
		myjob.setNumReduceTasks(1);
		//myjob.setCombinerClass(Reduce.class);
		myjob.setJobName("Module 2");
		myjob.setInputFormatClass(WholeInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(Text.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(args[1]));
		return(myjob.waitForCompletion(true)?0:1);
	}

}
