import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class IDF extends Configured implements Tool {
	public static class Map extends Mapper<Text,Text,Text,Text>
	{
		private static int D = 1;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			//System.out.println("Inside setup method");
			D = context.getConfiguration().getInt("numOfdocs",0);
			//System.out.println("D is:"+D);
		}
		@Override
		protected void map(Text key, Text value, Context context)throws IOException, InterruptedException {
			//System.out.println("Inside Module 4 Mapper");
			//String line = key.toString();
			String val = value.toString();
			//String word = line.split(",")[0];
			//String docname = line.split(",")[1];
			double n = Double.parseDouble(val.split(",")[0]);
			double max = Double.parseDouble(val.split(",")[1]);
			double N = Double.parseDouble(val.split(",")[2]);
			double m = Double.parseDouble(val.split(",")[3]);
			//System.out.println("n is:"+n+"max is:"+max+" N is:"+N+" m is:"+m+" D is:"+D);
			double tf = n/max;
			//System.out.println("TF is:"+tf);
			//String TF = "tf: "+tf;
			double idf = (Math.log(D/m)/Math.log(2));
			double tfidf = tf*idf;
			//System.out.println("IDF is:"+idf);
			//String IDF = "idf: "+idf;
			String word = key.toString().split(",")[0];
			String article = key.toString().split(",")[1];
			String new_val = word+","+tf+","+idf+","+tfidf;
			context.write(new Text(article),new Text(new_val));
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>
	{
		HashMap<String,Double>map = new HashMap();
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
		{
			//System.out.println("Inside Reducer. Article is:"+key.toString());
			for(Text val:values)
			{
				String line = val.toString();
				String word = line.split(",")[0];
				//System.out.println("Word is:"+word);
				String article = key.toString();
				double tf = Double.parseDouble(line.split(",")[1]);
				//System.out.println("TF is:"+tf);
				double idf = Double.parseDouble(line.split(",")[2]);
				double tfidf = Double.parseDouble(line.split(",")[3]);
				//System.out.println("IDF is:"+idf);
				String new_key = article+","+word+","+tf+","+idf;
				map.put(new_key, tfidf);
			}
			Comparator<String> comparator1 = new StringValueComparator(map);
			TreeMap<String,Double> TopFive = new TreeMap<String,Double>(comparator1);
			TopFive.putAll(map);
			
			Iterator<Entry<String,Double>> iter = TopFive.entrySet().iterator();
			Entry<String,Double> entry = null;
			while(TopFive.size()>10)
			{
				entry = iter.next();
				iter.remove();
			}
			//System.out.println("Size of HashMap:"+map.size()+"Size after deletion:"+TopFive.size());
			while(iter.hasNext())
			{
				Entry<String,Double> temp = iter.next();
				String val = temp.getKey();
				double tfidf = temp.getValue();
				String word = val.split(",")[1];
				String article = val.split(",")[0];
				double tf = Double.parseDouble(val.split(",")[2]);
				double idf = Double.parseDouble(val.split(",")[3]);
				String new_val = "Word--->"+word+",Term Frequency--->"+tf+",IDF--->"+idf+",TFIDF--->"+tfidf;
				context.write(new Text(article), new Text(new_val));
			}
		}
	}

	public int run(String[] args) throws Exception {
		System.out.println("Inside Module 2:Input->"+args[0]+" Output->"+args[1]);
		Configuration conf = new Configuration();
		int numOfdocs = Integer.parseInt(args[2]);
		conf.setInt("numOfdocs",numOfdocs);
		Job myjob4 = new Job(conf);
		myjob4.setMapperClass(Map.class);
		myjob4.setReducerClass(Reduce.class);
		myjob4.setNumReduceTasks(5);
		//myjob3.setCombinerClass(Reduce_M3.class);
		myjob4.setJarByClass(IDF.class);
		myjob4.setJobName("Module 2");
		myjob4.setInputFormatClass(KeyValueTextInputFormat.class);
		myjob4.setOutputFormatClass(TextOutputFormat.class);
		myjob4.setOutputKeyClass(Text.class);
		myjob4.setOutputValueClass(Text.class);
		//System.out.println("Input File Path:"+args[2]+" output file:"+args[3]);
		FileInputFormat.addInputPath(myjob4, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob4, new Path(args[1]));
		//D = args[2];
		return (myjob4.waitForCompletion(true)?0:1);
	}

}

