import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Creates an inverted positional index
 * For each word prints frequency in every file
 * it appears in
 */
public class InvertedPosIndex {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "InvertedPosIndex");
		job.setJarByClass(InvertedPosIndex.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(ListReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CustomValue.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
