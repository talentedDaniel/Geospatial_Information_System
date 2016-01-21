package geospatial1.operation1;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSWriter extends Configured implements Tool {
    
    public static final String FS_PARAM_NAME = "fs.default.name";
    
    public int run(String[] args) throws Exception {
        String[][] a1 = {
	        		{"hdfs://master:54310//data//ConvexHullTestData.csv","/home/danielvm/Desktop/Data/test/ConvexHullTestData.csv"},
	        		{"hdfs://master:54310//data//FarthestPairandClosestPairTestData.csv","/home/danielvm/Desktop/Data/test/FarthestPairandClosestPairTestData.csv"},
	        		{"hdfs://master:54310//data//JoinQueryPoint.csv","/home/danielvm/Desktop/Data/test/JoinQueryPoint.csv"},
	        		{"hdfs://master:54310//data//JoinQueryRectangle.csv","/home/danielvm/Desktop/Data/test/JoinQueryRectangle.csv"},
	        		{"hdfs://master:54310//data//PolygonUnionTestData.csv","/home/danielvm/Desktop/Data/test/PolygonUnionTestData.csv"},
	        		{"hdfs://master:54310//data//RangeQueryTestData.csv","/home/danielvm/Desktop/Data/test/RangeQueryTestData.csv"}
        		};
        
        for(int i=0; i<6; i++){
        	URI uri=URI.create(a1[i][0]);
            String localInputPath = a1[i][1];
            Path outputPath = new Path(uri);
            
            Configuration conf = new Configuration();
            conf.set("fs.default.name","hdfs://master:54310//data");

            FileSystem fs = FileSystem.get(conf);

            OutputStream os = fs.create(outputPath);
         
            InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));
            IOUtils.copyBytes(is, os, conf);
        }
        return 0;
    }

    public static void main( String[] args ) throws Exception {
    	int returnCode = ToolRunner.run(new HDFSWriter(), args);
        System.exit(returnCode);
    }
}