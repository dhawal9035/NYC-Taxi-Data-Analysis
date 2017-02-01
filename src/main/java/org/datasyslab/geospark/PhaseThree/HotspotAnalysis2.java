package org.datasyslab.geospark.PhaseThree;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class HotspotAnalysis2 {
	public static JavaSparkContext sc;
	static GeometryFactory fact = new GeometryFactory();
	static SparkConf conf;
	static String masterName;
	static String jarPath;
    
    public static void parseFunc(String inputFile, String outputFile) throws IOException{
    	JavaRDD<Tuple2<CubeId, Integer>> mappedRDD = readAndMapFile(inputFile);
    	
		JavaPairRDD<CubeId, Integer> pairedRDD = JavaPairRDD.fromJavaRDD(mappedRDD);
		
		JavaPairRDD<CubeId, Integer> filteredRDD = pairedRDD.filter(new Function<Tuple2<CubeId, Integer>,Boolean>() {

			@Override
			public Boolean call(Tuple2<CubeId,Integer> tupleData) throws Exception {
				if(tupleData._2 == 0) {
	                return false;
	            }
	            return true;
			}
	    });
		JavaPairRDD<CubeId, Iterable<Integer>> groupedRDD = filteredRDD.groupByKey();
        JavaPairRDD<CubeId, Integer> finalPairedRDD = groupedRDD.mapToPair(new PairFunction<Tuple2<CubeId, Iterable<Integer>>, CubeId, Integer>() {

			@Override
			public Tuple2<CubeId, Integer> call(
					Tuple2<CubeId, Iterable<Integer>> tupleData) throws Exception {
				Iterator<Integer> it = tupleData._2.iterator();
                int count = 0;
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
                return new Tuple2<CubeId, Integer>(tupleData._1, Integer.valueOf(count));
			}
        });
        Map<CubeId, Integer> map = finalPairedRDD.collectAsMap();
        HashMap<CubeId, Integer> calculationMap = new HashMap<CubeId, Integer>(map);
        CalculateZScore cz = new CalculateZScore();
        cz.calculateZScore(calculationMap, outputFile);
    }
    
    public static JavaRDD<Tuple2<CubeId, Integer>> readAndMapFile(String inputFile){
		JavaRDD<Tuple2<CubeId, Integer>> mappedRDD = sc.textFile(inputFile).map(
				new Function<String,Tuple2<CubeId, Integer>>() {
					@Override
					public Tuple2<CubeId,Integer> call(String line) throws Exception {
						String[] fields = line.split(",");
						Point p = null;
						if (!fields[5].equalsIgnoreCase("pickup_longitude")) {
							double x1 = Double.valueOf(fields[6]);
							int x2 = (int) (x1 * 100);
							x1 = x2 / 100.0;
							double y1 = Double.valueOf(fields[5]);
							int y2 = (int) (y1 * 100);
							y1 = y2 / 100.0;
							p = fact.createPoint(new Coordinate(x1, y1));
							p.setUserData(new Random());
						} else {
							p = fact.createPoint(new Coordinate(0.0, 0.0));
							p.setUserData(new Random());
							// return new Tuple2<String, Point>("32", p);
						}
						
						String eachDay = fields[1].split(" ")[0].split("-")[2];
						if(eachDay.charAt(0) == '0') {
							eachDay = eachDay.substring(1);
		                }
						CubeId c = new CubeId((Integer.valueOf(eachDay) - 1), p.getX(), p.getY());
						double x1 = 4050;
						double x2 = 4090;
						double y1 = -7425;
						double y2 = -7370;
						double nx1 = c.x1 * 100;
						double ny1 = c.y1 * 100;
						boolean flag = false;
						if ((nx1 <= 4090 && nx1 >= 4050) && (ny1 <= -7370 && ny1 >= -7425)) {
							flag = true;
		                } 

						if (flag == false) {
							return new Tuple2<CubeId, Integer>(c, 0);
						} else {
							return new Tuple2<CubeId, Integer>(c, 1);
						}
					}
				});
		
		return mappedRDD;
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		masterName = "spark://master:7077";
		//jarPath = "/home/osboxes/workspace/geospark/GeoSpark/target/geospark-0.3.jar";
		conf = new SparkConf()
				.setAppName("Phase3")
				.setMaster(masterName);
		sc = new JavaSparkContext(conf);
		//sc.addJar(jarPath);
		HotspotAnalysis2.parseFunc(args[0],args[1]);
	}
}
