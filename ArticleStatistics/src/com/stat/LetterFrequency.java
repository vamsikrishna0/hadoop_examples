package com.stat;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class LetterFrequency extends Configured implements Tool {
	
	public static class Map extends Mapper<Text,Text,Text,Text>
	{
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
				for(int i=0;i<word.length();i++)
				{
					char c = word.charAt(i);
					if(letters.containsKey(c))
						letters.put(c, letters.get(c)+1);
					else
						letters.put(c, 1);
					N=N+1;
				}
			}
			Comparator<Character> comparator1 = new CharValueComparatorDesc(letters);
			Comparator<Character> comparator2 = new CharValueComparatorAsc(letters);
			TreeMap<Character,Integer> TopTen = new TreeMap<Character,Integer>(comparator1);
			TopTen.putAll(letters);
			//System.out.println("Size of TopTen Map:"+TopTen.size());
			TreeMap<Character,Integer> BottomTen = new TreeMap<Character,Integer>(comparator2);
			BottomTen.putAll(letters);
			//System.out.println("Size of Bottom Map:"+BottomTen.size());
			
			Iterator<Entry<Character,Integer>> iter1 = TopTen.entrySet().iterator();
			Iterator<Entry<Character,Integer>> iter2 = BottomTen.entrySet().iterator();
			Entry<Character,Integer> entry = null;
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
				Entry<Character,Integer> temp = iter1.next();
				Character c = temp.getKey();
				int n = temp.getValue();
				double percent = (n/N)*100;
				String new_key = key.toString()+","+c;
				String new_val = "Lowest Count---->"+n+","+N+","+percent;
				count++;
				context.write(new Text(new_key), new Text(new_val));
			}
			//System.out.println("Size of Bottom Map after delete"+BottomTen.size());
			count = 0;
			//Bottom Ten words wrt count
			while(iter2.hasNext() && count<10)
			{
				Entry<Character,Integer> temp = iter2.next();
				Character c = temp.getKey();
				int n = temp.getValue();
				double percent = (n/N)*100;
				String new_key = key.toString()+","+c;
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
		System.out.println("Inside Module 4:Input->"+args[0]+" Output->"+args[1]);
		Configuration conf = new Configuration();
		Job myjob = new Job(conf);
		myjob.setMapperClass(Map.class);
		myjob.setReducerClass(Reduce.class);
		myjob.setJarByClass(LetterFrequency.class);
		myjob.setNumReduceTasks(1);
		//myjob.setCombinerClass(Reduce.class);
		myjob.setJobName("Module 4");
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

