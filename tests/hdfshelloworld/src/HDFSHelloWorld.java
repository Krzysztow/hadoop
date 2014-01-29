import java.io.File;
import java.io.IOException;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class HDFSHelloWorld extends Configured implements Tool {
  public static final String message = "Hello, world!\n";

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HDFSHelloWorld(), args);
		System.exit(exitCode);
	}

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    System.out.println("Default filesystem: " + conf.get("fs.default.name"));

	if (1 != args.length) {
		System.err.println("File path not provided");
		return 1;
	}
	
    Path filenamePath = new Path(args[0]);
	System.out.println("The path is: " + filenamePath);
	
    if (fs.exists(filenamePath)) {
   		//file exists, return
   		System.err.println("The given file (" + filenamePath + ") exists. Will not modify it");
		return 2;
    }

	try {
		FSDataOutputStream out = fs.create(filenamePath);
		out.writeUTF(message);
		out.close();

		FSDataInputStream in = fs.open(filenamePath);
		String messageIn = in.readUTF();
		System.out.print(messageIn);
		in.close();
	} catch (IOException ioe) {
		System.err.println("IOException during operation: " + ioe.toString());
		return 3;
	}
	
	return 0;
}
}
