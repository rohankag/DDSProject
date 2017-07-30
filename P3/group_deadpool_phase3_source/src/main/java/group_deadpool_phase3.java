//Group Deadpool
//The class takes two input arguments i.e. Input and Output file paths
//The final output are the top 50 co-ordinates with highest Getis-Ord Score

//Import statements for JAVA Utilities
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.io.Serializable;


//Import statements for Spark Utilities
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

//starting of main class
public class group_deadpool_phase3 {	
	final static long total_Cells = 68200; //total cells
	
	//Comparator class to set the order of Z-scores
	public static class z_score_Comparator implements Serializable, Comparator<Tuple2<String, Double>> {
		private static final long serialVersionUID = -6568983818709690932L;

		//This is an override function for compare method
		//Used to compare Tuples for ordering purposes
		@Override
		public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {		
			return o2._2.compareTo(o1._2);
		}
	}	

	 //Function to compute the standard deviation of the cells from the cube
	private static double get_Deviation(double xbar, Map<String, Double> cubes) {
		double sum_xbars = 0;

		for (String str : cubes.keySet()) {
			if (cubes.get(str) != null)
				sum_xbars += cubes.get(str) * cubes.get(str);
		}
		return Math.sqrt(sum_xbars / total_Cells - Math.pow(xbar, 2));  //standard deviation
	}

	//Function to calculate the mean of cell attribute
	private static double get_Xbar(Map<String, Double> cubes) {
		double sum = 0;

		for (String str : cubes.keySet()) {
			if (cubes.get(str)!= null)
				sum = sum + cubes.get(str);
		}
		return sum / total_Cells; //returns mean
	}
	
	//check if the point is in geo-envelope boundary and accordingly return true-false
	private static boolean in_Bound(Double longitude, Double latitude) {
		//Setting the lattitude and longitude range
		double min_Lati = 40.5;
		double min_Longi = -74.25;

		double max_Lati = 40.9;
		double max_Longi = -73.7;
		
		if((latitude <= max_Lati && latitude >= min_Lati) 
				&& (longitude >= min_Longi && longitude <= max_Longi)) {
			return true;
		} else {
			return false;
		}
	}

	//Main class function starts here
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("Debug 0: Connection Successful");
        System.out.println("Debug 1: Starting...");

		String input_File = args[0];
		String output_File = args[1];
		
		//Context configuration
		SparkConf conf = new SparkConf().setAppName("group_deadpool_phase3").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//storing input to JavaRDD
		JavaRDD<String> journey = sc.textFile(input_File);
		String header = journey.first(); //fetching header
		//removing header here
		JavaRDD<String> row = journey.filter(line -> !line.equals(header));	
		
		//Map phase: Used MaptoPair function to create a JavaPairRDD by extracting longitude, latitude and day parameter
		//and filtering them according to geo-envelope boundary i.e. by checking if the point is bounded or unbounded
		JavaPairRDD<String, Double> tuple = row.mapToPair(new PairFunction<String, String, Double>() {
			private static final long serialVersionUID = -4687444878386871018L;

			@Override
			public Tuple2<String, Double> call(final String line) throws Exception {				
				double longitude = Double.parseDouble(line.split(",")[5]); //fetching longitude
				double latitude = Double.parseDouble(line.split(",")[6]);  //fetching latitude
				
				int longitude_Int = (int) Math.floor(longitude*100);
				int latitude_Int=(int) Math.floor(latitude*100);
				int day_Int = Integer.parseInt(line.split(",")[1].split(" ")[0].split("-")[2]);
				
				//if the point is bounded then add to the tuple with the value1
				if(in_Bound(longitude, latitude)) {
					return new Tuple2<String, Double>(String.valueOf(latitude_Int) 
									+ "," + String.valueOf(longitude_Int) 
										+ "," + String.valueOf(day_Int), Double.valueOf(1));
				} else {
					return new Tuple2<String, Double>(String.valueOf(latitude_Int) 
									+ "," + String.valueOf(longitude_Int) 
										+ "," + String.valueOf(day_Int), Double.valueOf(0));
				}
			}
		});
		
		//filter the tuple and reduce by the key to sum up all the journeys and calculate the 
		//cell attribute value xj which is further used to calculate the Z-score.
		tuple = tuple.filter(new Function<Tuple2<String, Double>, Boolean>() {
			private static final long serialVersionUID = -6031652226521469760L;

			
			public Boolean call(Tuple2<String, Double> t) throws Exception {
				if (t._2 == 0) {
					return false;
				}
				return true;
			}
		}).reduceByKey((x,y)->x+y); //reducing by key
		
		final Map<String, Double> cubes = tuple.collectAsMap();
		double xbar = get_Xbar(cubes); //computing mean
		double standard_Deviation = get_Deviation(xbar,cubes); //computing standard deviation

		//Computing the Z-score and forming tuples with trip data and z-score as key and value
		JavaPairRDD<String, Double> z_scores_Tuples = tuple.mapToPair(new PairFunction<Tuple2<String, Double>, String, Double>() {
					private static final long serialVersionUID = 9039112934335164463L;

					public Tuple2<String, Double> call(final Tuple2<String, Double> cell) {
						if (cell._1 == null) {
							return new Tuple2<String, Double>(null, null);
						}
                        
						//Parsing Longitude, latitude and time
						int latitude_Int = Integer.parseInt(cell._1.split(",")[0]);
						int longitude_Int = Integer.parseInt(cell._1.split(",")[1]);
						int day_Int = Integer.parseInt(cell._1.split(",")[2]);

						StringBuffer final_String = new StringBuffer();
						final_String.append((double)latitude_Int / 100)
										.append(",").append((double)longitude_Int / 100)
											.append(",").append(day_Int - 1);

						double sum = 0;
						double neighbors = 0;
			            
				//finding the neighbors and sum of attribute values which is used to calculate the Z-score
						for (int day = -1; day < 2; day++) {
							for (int longi = -1; longi < 2; longi++) {
								for (int  lati = -1; lati < 2; lati++) {
									StringBuffer buff_String = new StringBuffer();
									int newLat = latitude_Int + lati;
									int newlong = longitude_Int + longi;

									buff_String.append(newLat).append(',').append(newlong)
															.append(',').append(day_Int + day);

									String tuple_Value = buff_String.toString();

									if(tuple_Value != null){
										if (cubes.containsKey(tuple_Value)) {
											double count = cubes.get(tuple_Value);
											sum = sum + count;
											neighbors++;
										}
									}
								}
							}
						}
						
						//computing the Z-score with the given formulae
						double z_score = (sum - (xbar * neighbors)) / 
											(standard_Deviation * Math.sqrt((total_Cells 
												* neighbors - neighbors * neighbors) / (total_Cells - 1)));
						
						return new Tuple2<String, Double>(final_String.toString(), z_score);
					}
				});
		
		//ordering the Z-scores by comparator and storing to list-tuples
		List<Tuple2<String, Double>> list_Tuples = z_scores_Tuples.takeOrdered(50,  new z_score_Comparator());		
		
		 List<String> result_array = new ArrayList<String>();
		 for(Tuple2<String, Double> list:list_Tuples) {
			 result_array.add(list.toString().replace("(", "").replace(")", ""));
		 }
		
		JavaRDD<String> output = sc.parallelize(result_array);		

		System.out.println("Debug 3: Completed Successfully");	
		output.saveAsTextFile(output_File);		
		sc.close();
		System.out.println("Debug 4: Output files created");
	}
}


