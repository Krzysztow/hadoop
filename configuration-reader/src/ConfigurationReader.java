//package org.myorg;


import java.util.Map;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

public class ConfigurationReader extends Configured implements Tool {
  
  static {
    //Configuration.addDefaultResource("hdfs-default.xml");
    //Configuration.addDefaultResource("hdfs-site.xml");
    //Configuration.addDefaultResource("mapred-default.xml");
    //Configuration.addDefaultResource("mapred-site.xml");
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    for (Map.Entry<String, String> entry: conf) {
      System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
    }
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ConfigurationReader(), args);
    System.exit(exitCode);
  }
}
