package com.stat;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class CharacterCount extends Configured implements Tool{
	/*HADOOP job used to count each and every character in all the articles.
	 * 
	 */
	private static Configuration conf;

	public static class Map extends Mapper<Text,Text,Text,Text>
	{
		/* Input to the mapper is the entire file from the WholeFileRecordReader
		 * Key is the file name and the value is the content of the file
		 * Output from the mapper word count and letter count in each article
		 */
		public void map(Text key,Text value,Context context)throws IOException,InterruptedException
		{
			//System.out.println("Inside Module 1 mapper");
			int lettercnt=0;
			StringTokenizer tokens = new StringTokenizer(value.toString());
			while(tokens.hasMoreTokens())
			{
				lettercnt = lettercnt+tokens.nextToken().length();
			}
			String stats = key.toString()+","+lettercnt;
			//System.out.println("File is:"+key.toString()+" value is:"+stats);
			context.write(new Text("values"), new Text(stats));
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>
	{
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
		{
			//System.out.println("Inside Module 1 reducer. Key is:"+key.toString());
			int max_letters=0;
			int min_letters=0;
			boolean min_flag = false;
			String maxArticle = null;
			String minArticle = null;
			for(Text val:values)
			{
				//System.out.println("Inside For loop");
				String tmp = val.toString();
				int lettercnt = Integer.parseInt(tmp.split(",")[1]);
				//System.out.println("Number of words:"+words);
				if(min_flag==false)
				{
					minArticle = key.toString();
					min_letters = lettercnt; 
					min_flag = true;
				}
				//System.out.println("Value found. Words:"+words+" Letters:"+letters);
				String article = tmp.split(",")[0];
				//System.out.println("Article is:"+article);
				if(max_letters < lettercnt)
				{
					max_letters = lettercnt;
					maxArticle = article;
				}
				else if(min_letters > lettercnt)
				{
					min_letters = lettercnt;
					minArticle = article;
				}
			}
			String new_val1 = "Letters->"+maxArticle+","+max_letters;
			String new_val2 = "Letters->"+minArticle+","+min_letters;
			context.write(new Text("Max Article"), new Text(new_val1));
			context.write(new Text("Min Article"), new Text(new_val2));
		}
	}

	public int run(String[] args) throws Exception {
		System.out.println("Inside Module 3:Input->"+args[0]+" Output->"+args[1]);
		Configuration conf = new Configuration();
		Job myjob = new Job(conf);
		myjob.setMapperClass(Map.class);
		myjob.setReducerClass(Reduce.class);
		myjob.setJarByClass(WordCount.class);
		//myjob.setCombinerClass(Reduce.class);
		myjob.setNumReduceTasks(1);
		myjob.setJobName("Module 3");
		myjob.setInputFormatClass(WholeInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(args[1]));
		return(myjob.waitForCompletion(true)?0:1);
	}


}
