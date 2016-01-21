package geospatial1.operation1;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

public class ClosestPair {
	  public static class Point
	  {
	    public double x;
	    public double y;

	    public Point(double x, double y)
	    {
	      this.x = x;
	      this.y = y;
	    }
	  }
	static class ParsePoint implements Function<String, Tuple2<Double,Double>>,  Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = -4555656803583860768L;
		private static final Pattern SPACE = Pattern.compile(",");
	
		public Tuple2<Double,Double> call(String line) {
			String[] tok = SPACE.split(line);
	    	
	    	Double x = Double.parseDouble(tok[2]);
	    	Double y = Double.parseDouble(tok[3]);
	    	return new Tuple2<Double,Double>(x,y);
	    }
	}
	
	private static class sortX implements Function<Tuple2<Double, Double>, Double>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Double call(Tuple2<Double,Double> a) {
			return a._1;
		}
	}
	
	
	public static double distance(Tuple2<Double,Double> p1, Tuple2<Double,Double> p2)
	{
		double xdist = p2._1 - p1._1;
		double ydist = p2._2 - p1._2;
		return Math.pow(xdist, 2) + Math.pow(ydist, 2);
	}
	
	
	public static void sortByX(List<? extends Tuple2<Double,Double>> points)
	{
	  Collections.sort(points, new Comparator<Tuple2<Double,Double>>() {
	      public int compare(Tuple2<Double,Double> point1, Tuple2<Double,Double> point2)
	      {
	        if (point1._1 < point2._1)
	          return -1;
	        if (point1._1 > point2._1)
	          return 1;
	        return 0;
	      }
	    }
	  );
	}
	  

	  public static void sortByY(List<? extends Tuple2<Double,Double>> points)
	  {
	    Collections.sort(points, new Comparator<Tuple2<Double,Double>>() {
	        public int compare(Tuple2<Double,Double> point1, Tuple2<Double,Double> point2)
	        {
	          if (point1._2 < point2._2)
	            return -1;
	          if (point1._2 > point2._2)
	            return 1;
	          return 0;
	        }
	      }
	    );
	  }
	  
	  public static Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> bruteForce(List<? extends Tuple2<Double,Double>> points)
	  {
	    int numPoints = points.size();
	    if (numPoints < 2)
	      return null;
	    Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> pair = new Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double>(points.get(0), points.get(1),distance(points.get(0),points.get(1)));
	    if (numPoints > 2)
	    {
	      for (int i = 0; i < numPoints - 1; i++)
	      {
	    	Tuple2<Double,Double> point1 = points.get(i);
	        for (int j = i + 1; j < numPoints; j++)
	        {
	          Tuple2<Double,Double> point2 = points.get(j);
	          double distance = distance(point1, point2);
	          if (distance < pair._3())
	            pair = new Tuple3(point1, point2, distance);
	        }
	      }
	    }
	    return pair;
	  }
	  
	  public static Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> divideAndConquer(List<? extends Tuple2<Double,Double>> points)
	  {
	    List<Tuple2<Double,Double>> pointsSortedByY = new ArrayList<Tuple2<Double,Double>>(points);
	    sortByY(pointsSortedByY);
	    return divideAndConquer(points, pointsSortedByY);
	  }
	  
	  private static Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> divideAndConquer(List<? extends Tuple2<Double,Double>> pointsSortedByX, List<? extends Tuple2<Double,Double>> pointsSortedByY)
	  {
	    int numPoints = pointsSortedByX.size();
	    if (numPoints <= 3)
	      return bruteForce(pointsSortedByX);

	    int dividingIndex = numPoints >>> 1;
	    List<? extends Tuple2<Double, Double>> leftOfCenter = pointsSortedByX.subList(0, dividingIndex);
	    List<? extends Tuple2<Double, Double>> rightOfCenter = pointsSortedByX.subList(dividingIndex, numPoints);
	    List<Tuple2<Double,Double>> tempList = new ArrayList<Tuple2<Double,Double>>(leftOfCenter);
	    sortByY(tempList);
	    Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> closestPair = divideAndConquer(leftOfCenter, tempList);

	    tempList.clear();
	    tempList.addAll(rightOfCenter);
	    sortByY(tempList);
	    Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> closestPairRight = divideAndConquer(rightOfCenter, tempList);

	    if (closestPairRight._3() < closestPair._3())
	      closestPair = closestPairRight;

	    tempList.clear();
	    double shortestDistance =closestPair._3();
	    double centerX = rightOfCenter.get(0)._1;
	    for (Tuple2<Double,Double> point : pointsSortedByY)
	      if (Math.abs(centerX - point._1) < shortestDistance)
	        tempList.add(point);

	    for (int i = 0; i < tempList.size() - 1; i++)
	    {
	      Tuple2<Double,Double> point1 = tempList.get(i);
	      for (int j = i + 1; j < tempList.size(); j++)
	      {
	    	Tuple2<Double,Double> point2 = tempList.get(j);
	        if ((point2._2 - point1._2) >= shortestDistance)
	          break;
	        double distance = distance(point1, point2);
	        if (distance < closestPair._3())
	        {
	          closestPair = new Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double>(point1, point2, distance);
	          shortestDistance = distance;
	        }
	      }
	    }
	    return closestPair;
	  }
	  
	private static class DcAlgorithm implements Function2<Integer, Iterator<Tuple2<Double,Double>>, Iterator<Tuple5<Integer, Tuple2<Tuple2<Double,Double>,Tuple2<Double,Double>>, Double, Tuple2<Double,Double>, List<Tuple2<Double,Double>>>>>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = -3392056000962818174L;

		public Iterator<Tuple5<Integer, Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Double, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>> call(Integer a, Iterator<Tuple2<Double, Double>> p)
				throws Exception {
			// TODO Auto-generated method stub
			ArrayList<Tuple5<Integer, Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Double, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>> res = new ArrayList<Tuple5<Integer, Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Double, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>>();
			// pointList contains points in each partition
			List<Tuple2<Double,Double>> pointList = new ArrayList<Tuple2<Double,Double>>();
			double minX, maxX;
			int count = 0;
			while(p.hasNext())
			{
				pointList.add(p.next());
				count = count + 1;
			}
			minX = pointList.get(0)._1;
			maxX = pointList.get(count-1)._1;
			
			Tuple3<Tuple2<Double,Double>,Tuple2<Double,Double>,Double> minDistanceWithPoints = divideAndConquer(pointList);
			
			List<Tuple2<Double, Double>> leftPoints = new ArrayList<Tuple2<Double, Double>>();
			if(a == 0){
				GetLeftPoints(a, leftPoints, pointList, minDistanceWithPoints._3(), maxX);
			}else{
				GetLeftPoints(a, leftPoints, pointList, minDistanceWithPoints._3(), minX);
			}
			
			res.add(new Tuple5<Integer, Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Double, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>(a, new Tuple2(minDistanceWithPoints._1(),minDistanceWithPoints._2()), minDistanceWithPoints._3(), new Tuple2(minX, maxX), leftPoints));
			return res.iterator();
		}

	}
	
	private static void GetLeftPoints(Integer index, List<Tuple2<Double, Double>> leftPoints, List<Tuple2<Double, Double>> points, Double minDis, Double boundaryX){
		if(index == 0){
			for(int i=points.size()-1; i>=0;i--){
				if(boundaryX - points.get(i)._1() < minDis){
					leftPoints.add(points.get(i));
				}else{
					break;
				}
			}
		}else{
			for(int i=0; i<points.size(); i++){
				if(points.get(i)._1() - boundaryX < minDis){
					leftPoints.add(points.get(i));
				}
				else{
					break;
				}
			}
		}
	}
	
	private static Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double> GetGlobalMinDistance(Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double> p1, Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double> p2){
		Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double> results = new Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double>(new Tuple2(0.0, 0.0), new Tuple2(0.0, 0.0), 0.0);
		results = p1._3()<p2._3() ? p1 : p2;
		return results;
	}
	
	private static Double GetDistance(Tuple2<Double, Double> p1, Tuple2<Double, Double> p2){
		return Math.sqrt(Math.pow(p1._1()-p2._1(), 2) + Math.pow(p1._2()-p2._2(), 2));
	}
	
	private static Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double> GetFinalResults(List<Tuple2<Double, Double>> l1, List<Tuple2<Double, Double>> l2, Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double> min){
		for(int i=0; i<l1.size(); i++){
			for(int j=0; j<l2.size(); j++){
				Double dis = GetDistance(l1.get(i), l2.get(j));
				if(dis < min._3()){
					min = new Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double>(new Tuple2(l1.get(i)._1(), l1.get(i)._2()), new Tuple2(l2.get(j)._1(), l2.get(j)._2()), dis);
				}
			}
		}
		return min;
	}

	public static void main(String[] args) throws Exception {
			Integer numPartition = 2;
			SparkConf sparkConf = new SparkConf().setMaster("spark://192.168.124.131:7077").setAppName("ClosetPair");
			JavaSparkContext jsc = new JavaSparkContext(sparkConf);
			jsc.addJar("/home/danielvm/workspace/operation1/target/uber-operation1-0.0.1-SNAPSHOT.jar");
			JavaRDD<String> file = jsc.textFile("hdfs://master:54310//data//FarthestPairandClosestPairTestData.csv");
			JavaRDD<Tuple2<Double,Double>> points = file.map(new ParsePoint()).sortBy(new sortX(), true, numPartition).cache();
			points = points.distinct();
			
			JavaRDD<Tuple5<Integer, Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Double, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>> res = points.mapPartitionsWithIndex(new DcAlgorithm(), true);
			List<Tuple5<Integer, Tuple2<Tuple2<Double, Double>, Tuple2<Double, Double>>, Double, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>> partialResults = res.collect();
			
			Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double> p1 = new Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double>(new Tuple2(partialResults.get(0)._2()._1()._1(), partialResults.get(0)._2()._1()._2()), new Tuple2(partialResults.get(0)._2()._2()._1(), partialResults.get(0)._2()._2()._2()), partialResults.get(0)._3());
			Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double> p2 = new Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double>(new Tuple2(partialResults.get(1)._2()._1()._1(), partialResults.get(1)._2()._1()._2()), new Tuple2(partialResults.get(1)._2()._2()._1(), partialResults.get(1)._2()._2()._2()), partialResults.get(1)._3());
			Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double> min = GetGlobalMinDistance(p1, p2);
			List<Tuple2<Double, Double>> leftPoints = partialResults.get(0)._5();
			List<Tuple2<Double, Double>> rightPoints = partialResults.get(1)._5();
			Tuple3<Tuple2<Double, Double>, Tuple2<Double, Double>, Double> answer = GetFinalResults(leftPoints, rightPoints, min);
			List<Tuple2<Double, Double>> res1=new ArrayList<Tuple2<Double, Double>>(2);
			res1.add(answer._1()); 
			res1.add(answer._2());
			JavaRDD<String> res2=jsc.parallelize(res1).map(new Function<Tuple2<Double, Double>, String>(){
				public String call(Tuple2<Double, Double> p) throws Exception{
					String s=p._1()+",  "+p._2();
					return s;
				}
			});
			// Save as Text File to HDFS
			Configuration hadoopConf=new org.apache.hadoop.conf.Configuration();
			FileSystem hdfs=org.apache.hadoop.fs.FileSystem.get(URI.create("hdfs://master:54310"), hadoopConf);
			String output="hdfs://master:54310//data//ClosestPair";
			try{
				hdfs.delete(new org.apache.hadoop.fs.Path(output), true);
			}
			catch(IOException e){
				throw e;		
			}
			res2.repartition(1).saveAsTextFile("hdfs://master:54310//data//ClosestPair");
	    	List<String> result=res2.collect();
	    	// Print Result
	    	for(int i=0; i<result.size(); i++){
	    		System.out.println(result.get(i));																																																																																																																																																										
	    	}
	    	
		    jsc.stop();
			System.out.println(answer._3());
		}
}
