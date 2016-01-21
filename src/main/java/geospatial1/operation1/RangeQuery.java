package geospatial1.operation1;


import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class RangeQuery {
	static class PointPairs implements Serializable{
		double[] x;
		double id;
		PointPairs(double id, double[] x){
			this.x=x; this.id=id;
		}
	}
	// parse results with id and (x1,y1,x2,y2) from CSV file
	static class ParsePointPairs implements Function<String, PointPairs>, Serializable{   
		private static final Pattern pattern=Pattern.compile(",");
		public PointPairs call(String line) throws Exception{
			int N=4;
			String[] tok=pattern.split(line);
			double id=Double.parseDouble(tok[1]);
			double[] x=new double[N];
			for(int i=0; i<N; i++){
				x[i]=Double.parseDouble(tok[i+2]);
			}
			return new PointPairs(id, x);
		}
	}
	public static void main(String args[]) throws IOException{
		SparkConf sparkConf = new SparkConf().setMaster("spark://192.168.124.131:7077").setAppName("RangeQuery");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.addJar("/home/danielvm/workspace/operation1/target/uber-operation1-0.0.1-SNAPSHOT.jar");
		JavaRDD<String> lines = sc.textFile("hdfs://master:54310//data//RangeQueryTestData.csv",4);
		final Broadcast<double[]> query=sc.broadcast(new double[] {-88,30,-81,33});
		//final Broadcast<double[]> query=sc.broadcast(new double[] {0,0,1,1});
		JavaRDD<PointPairs> points = lines.map(new ParsePointPairs());   // get id and (x1,y1,x2,y2)
		JavaRDD<PointPairs> result=points.filter(new Function<PointPairs, Boolean>(){
			public Boolean call(PointPairs p) throws Exception{
				if (Math.min(p.x[0], p.x[2])>=Math.min(query.value()[0], query.value()[2]) && Math.max(p.x[0], p.x[2])<=Math.max(query.value()[0], query.value()[2])
						&& Math.min(p.x[1], p.x[3])>=Math.min(query.value()[1], query.value()[3]) && Math.max(p.x[1], p.x[3])<=Math.max(query.value()[1], query.value()[3]))
					return true;
				else 
					return false;
			}
		});
		JavaRDD<String> str=result.map(new Function<PointPairs, String>(){
			public String call(PointPairs p) throws Exception{
				String s="ID: "+p.id+"  Coordinates:  "+ p.x[0]+", "+p.x[1]+
						";   "+p.x[2]+",  "+p.x[3];
				return s;
			}
		});
		// Save as Text File to HDFS
		Configuration hadoopConf=new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs=org.apache.hadoop.fs.FileSystem.get(URI.create("hdfs://master:54310"), hadoopConf);
		String output="hdfs://master:54310//data//RangeQuery";
		try{
			hdfs.delete(new org.apache.hadoop.fs.Path(output), true);
		}
		catch(IOException e){
			throw e;
		}
		str.repartition(1).saveAsTextFile("hdfs://master:54310//data//RangeQuery");
		// Print results
		List<String> res=str.collect();
		for (int i=0; i<res.size(); i++){
			System.out.println(res.get(i));
		}
	}
}
