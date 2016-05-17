package com.stat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class Driver {
	/* This is the main driver class
	 * all the jobs are called from this class
	 */
	static Configuration conf;
	public static void main(String[] args)throws Exception {
		conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Driver driver = new Driver();
		String inpath = args[0];
		fs.delete(new Path("wiki_word_stats"));
		int res = driver.runWC(inpath,"wiki_word_stats");
		if (res==1)
		{
			System.out.println("Job1 failed");
			System.exit(0);
		}
		fs.delete(new Path("wiki_tf"));
		res = driver.runTf(inpath,"wiki_tf");
		if (res==1)
		{
			System.out.println("Job2 failed");
			System.exit(0);
		}
		
		fs.delete(new Path("wiki_char_stats"));
		res = driver.runCC(inpath,"wiki_char_stats");
		if (res==1)
		{
			System.out.println("Job3 failed");
			System.exit(0);
		}
		fs.delete(new Path("wiki_cf"));
		res = driver.runLf(inpath,"wiki_cf");
		if (res==1)
		{
			System.out.println("Job4 failed");
			System.exit(0);
		}
	}
	private int runLf(String inpath, String outpath)throws Exception {
		String args[] = new String[2];
		args[0] = inpath;
		args[1] = outpath;
		int res = ToolRunner.run(conf,new LetterFrequency(), args);
		return res;
	}
	private int runCC(String inpath, String outpath)throws Exception {
		String args[] = new String[2];
		args[0] = inpath;
		args[1] = outpath;
		int res = ToolRunner.run(conf,new CharacterCount(), args);
		return res;
	}
	private int runTf(String inpath, String outpath)throws Exception {
		String args[] = new String[2];
		args[0] = inpath;
		args[1] = outpath;
		int res = ToolRunner.run(conf,new TermFrequency(), args);
		return res;
	}
	public int runWC(String inpath,String outpath) throws Exception
	{
		String args[] = new String[2];
		args[0] = inpath;
		args[1] = outpath;
		int res = ToolRunner.run(conf,new WordCount(), args);
		return res;
	}

}
