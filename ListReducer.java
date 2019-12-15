import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ListReducer extends Reducer<Text, CustomValue, Text, Text>{
	
	public void reduce(Text key, Iterable<CustomValue> values, Context context) throws IOException, InterruptedException {
		Map<String, List<Long>> result = new HashMap<>();
		
		for (CustomValue val : values) {
			Long position = val.getPosition();
			
			List<Long> oldPositions = result.get(val.getFilename());
			if (oldPositions == null) {
				List<Long> newEntry = new LinkedList<>();
				newEntry.add(position);
				result.put(val.getFilename(), newEntry);
			} else {
				oldPositions.add(position);
				result.put(val.getFilename(), oldPositions);
			}
		}
		
		Text outKey = new Text();
		Text outValue = new Text();
		
		outKey.set(key);
		
		
		// prints out all the values in the map
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		int current = 0;
		int length = result.size();
		for (Entry<String, List<Long>> e : result.entrySet()) {
			sb.append(e.getKey().toString()).append(": ").append(e.getValue().toString());
			if (!(current == length -1)) {
				sb.append(", ");
			}
				
			current++;
		}
		sb.append("}");
		
		outValue.set(sb.toString());
		

		context.write(outKey, outValue);
	}
}
