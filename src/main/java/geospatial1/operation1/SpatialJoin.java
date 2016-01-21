package geospatial1.operation1;

import geospatial1.operation1.InputClass.input1;
import geospatial1.operation1.InputClass.input2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class SpatialJoin {

	// constructor , getters and setters
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("SpatialJoinQuery")
				.setMaster("spark://192.168.124.131:7077");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.addJar("/home/danielvm/workspace/operation1/target/uber-operation1-0.0.1-SNAPSHOT.jar");

		JavaRDD<input1> rdd_records1 = sc
				.textFile("hdfs://master:54310//data//JoinQueryRectangle.csv",
						100).map(new Function<String, input1>() {
					public input1 call(String line) throws Exception {
						String[] fields = line.split(",");
						input1 sd1 = new input1(fields[1].trim(), fields[2]
								.trim(), fields[3].trim(), fields[4].trim(),
								fields[5].trim());

						return sd1;
					}
				}).cache();
		JavaRDD<input1> SortedPoint = rdd_records1.sortBy(
				new Function<input1, Double>() {
					public Double call(input1 p) throws Exception {
						return p.getx2();
					}
				}, true, 100);

		List<input1> list = new ArrayList<input1>();
		for (input1 rddData : SortedPoint.collect()) {
			list.add(rddData);
		}
		final Broadcast<List<input1>> list1 = sc.broadcast(list);
		JavaRDD<input2> rdd_records2 = sc
				.textFile("hdfs://master:54310//data//JoinQueryPoint.csv", 100)
				.map(new Function<String, input2>() {
					public input2 call(String line) throws Exception {
						String[] fields = line.split(",");
						input2 sd2 = new input2(fields[1].trim(), fields[2]
								.trim(), fields[3].trim(), fields[4].trim(),
								fields[5].trim());

						return sd2;
					}
				}).cache();
		JavaRDD<String> rdd_records22 = rdd_records2
				.mapPartitions(new FlatMapFunction<Iterator<input2>, String>() {
					public Iterable<String> call(Iterator<input2> s)
							throws Exception {
						int index = 0;
						List<String> idLines = new ArrayList<String>();
						String myString;
						while (s.hasNext()) {
							input2 a = s.next();
							myString = a.getid() + ": ";
							for (input1 l : list1.value()) {
								if (a.getx1() > l.getx2()
										&& a.getx2() > l.getx2()) {
									if (a.getx1() < l.getx1()
											&& a.getx2() < l.getx1()
											&& l.gety2() < a.gety1()
											&& a.gety1() < l.gety1()
											&& l.gety2() < a.gety2()
											&& a.gety2() < l.gety1()) {
										myString += l.getid() + ",";
									}
								} else {
									break;
								}
							}
							if (myString.endsWith(",")) {
								myString = myString.substring(0,
										myString.length() - 1);
							}
							idLines.add(myString);
							++index;
						}
						return idLines;
					}
				});
		// Save as Text File to HDFS
		Configuration hadoopConf=new org.apache.hadoop.conf.Configuration();
		FileSystem hdfs=org.apache.hadoop.fs.FileSystem.get(URI.create("hdfs://master:54310"), hadoopConf);
		String output="hdfs://master:54310//data//SpatialJoin";
		try{
			hdfs.delete(new org.apache.hadoop.fs.Path(output), true);
		}
		catch(IOException e){
			throw e;		
		}
		rdd_records22.repartition(1).saveAsTextFile("hdfs://master:54310//data//SpatialJoin");
    	List<String> result=rdd_records22.repartition(1).collect();
    	// Print Result
    	for(int i=0; i<result.size(); i++){
    		System.out.println(result.get(i));																																																																																																																																																										
    	}
	}
}
