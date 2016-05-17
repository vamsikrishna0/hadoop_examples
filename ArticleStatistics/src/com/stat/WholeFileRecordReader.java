package com.stat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<Text,Text>{
	
	private FileSplit fileSplit;
	private Configuration conf;
	private boolean processed = false;
	
	private Text key = new Text();
	private Text value = new Text();

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public Text getCurrentKey() throws IOException,InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException,InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed?1.0f:0.0f;
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)throws IOException, InterruptedException {
		this.fileSplit = (FileSplit)inputSplit;
		this.conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!processed)
		{
			byte[] contents = new byte[(int)fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			key = new Text(file.getName());
			FSDataInputStream in = null;
			try
			{
				in = fs.open(file);
				IOUtils.readFully(in,contents,0,contents.length);
				value = new Text(contents);
			}finally
			{
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

}
