import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;


public class CustomValue implements Writable{

	private String filename;
	private long position;
	
	
	public String getFilename() {
		return filename;
	}

	public void setFilename(String fileName) {
		this.filename = fileName;
	}

	public long getPosition() {
		return position;
	}

	public void setPosition(long position) {
		this.position = position;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		filename = arg0.readUTF();
		position = arg0.readLong();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(filename);
		arg0.writeLong(position);
	}	
}
