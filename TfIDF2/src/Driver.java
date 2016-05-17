
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
		String outpath = args[1];
		int numOfdocs = Integer.parseInt(args[2]);
		fs.delete(new Path("tfidf1"));
		int res = driver.runTf(args[0],"tfidf1");
		if(res==1)
		{
			System.out.println("Module 1 failed");
			System.exit(0);
		}
		fs.delete(new Path(outpath));
		res = driver.runIdf("tfidf1",outpath,numOfdocs);
		if(res==1)
		{
			System.out.println("Module 2 failed");
			System.exit(0);
		}
	}
	private int runIdf(String inpath, String outpath, Integer numOfdocs)throws Exception {
		String args[]= new String[3];
		args[0] = inpath;
		args[1] = outpath;
		args[2] = numOfdocs.toString();
		int res = ToolRunner.run(conf, new IDF(), args);
		return res;
	}
	private int runTf(String inpath, String outpath)throws Exception {
		String args[] = new String[2];
		args[0] = inpath;
		args[1] = outpath;
		int res = ToolRunner.run(conf,new TermFrequency(), args);
		return res;
	}
}
