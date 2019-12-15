import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MRBench.Map;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, CustomValue> {
	
	private HashMap<String, Integer> numOccurencies = new HashMap<>();;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
		
		// out key
		Text outKey = new Text();
		// out value
		CustomValue outValue = new CustomValue();
		
		numOccurencies.clear();
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String tok = itr.nextToken();
			
			// currently processed word
			outKey.set(tok);
			int offset = 0;
			
			if (numOccurencies.get(tok) == null) {
				numOccurencies.put(tok, value.toString().indexOf(tok));
				offset = value.toString().indexOf(tok);
			} else {
				int lastPos = numOccurencies.get(tok);
				numOccurencies.put(tok, value.toString().indexOf(tok, lastPos + 1));
				offset = value.toString().indexOf(tok, lastPos + 1);
			}
			
			FileSplit fs = (FileSplit) context.getInputSplit();
		    // name of the current file 
			String filename = fs.getPath().getName();
			outValue.setFilename(filename);
						
			outValue.setPosition(key.get() + offset);
			context.write(outKey, outValue);
			
		}
	}
	
}
