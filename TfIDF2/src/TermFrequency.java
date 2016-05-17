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
			int max = 0;
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
			max = (Collections.max(words.values()));
			//System.out.println("Maximum Frequency:"+max);
			Iterator<String> iter1 = words.keySet().iterator();
			while(iter1.hasNext())
			{
				String word = iter1.next();
				int n = words.get(word);
				String new_val = key.toString()+","+n+","+max+","+N+","+"1";
				context.write(new Text(word), new Text(new_val));
			}
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
		{
			//System.out.println("Inside Reducer:");
			ArrayList<String> listOfValues = new ArrayList();
			int cnt = 0;
			String word = key.toString();
			for(Text val:values)
			{
				//System.out.println("Value added"+val.toString());
				listOfValues.add(val.toString());
				cnt = cnt+Integer.parseInt(val.toString().split(",")[4]);
			}
			Iterator<String> iter1 = listOfValues.iterator();
			while(iter1.hasNext())
			{
				String line = iter1.next();
				String article = line.split(",")[0];
				int n = Integer.parseInt(line.split(",")[1]);
				int max = Integer.parseInt(line.split(",")[2]);
				double N = Double.parseDouble(line.split(",")[3]);
				String new_val = n+","+max+","+N+","+cnt;
				String new_key = word+","+article;
				context.write(new Text(new_key), new Text(new_val));
			}
		}
	}

	public int run(String[] args) throws Exception {
		System.out.println("Inside Module 1:Input->"+args[0]+" Output->"+args[1]);
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
