/**
 * FILE: JoinQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.JoinQuery.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgement;
import org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgementUsingIndex;
import org.datasyslab.geospark.joinJudgement.GeometryByRectangleJudgement;
import org.datasyslab.geospark.joinJudgement.AllByRectangleJudgementUsingIndex;
import org.datasyslab.geospark.joinJudgement.RectangleByRectangleJudgement;
import org.datasyslab.geospark.spatialPartitioning.DuplicatesHandler;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;


//import org.apache.spark.JavaSparkContext;
import org.apache.spark.SparkContext;

import scala.Tuple2;


// TODO: Auto-generated Javadoc
/**
 * The Class JoinQuery.
 */
public class JoinQuery implements Serializable{

	
    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Envelope, HashSet<Point>> SpatialJoinQuery(PointRDD spatialRDD,RectangleRDD queryRDD,boolean useIndex) throws Exception {

        if(useIndex)
        {
        	//Check if rawPointRDD have index.
            if(spatialRDD.indexedRDD == null) {
	            throw new Exception("[JoinQuery][SpatialJoinQuery] Index doesn't exist. Please build index.");
            }
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.indexedRDD.cogroup(queryRDD.spatialPartitionedRDD);

            //flatMapToPair, use HashSet.

            JavaPairRDD<Envelope, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new AllByRectangleJudgementUsingIndex());
            
            JavaPairRDD<Envelope, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByRectangle(joinResultWithDuplicates);
            
            JavaPairRDD<Envelope, HashSet<Point>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Point>>()
            {
				@Override
				public HashSet<Point> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Point)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            	
            });
            return castedResult;
        }
        else
        {
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.spatialPartitionedRDD.cogroup(queryRDD.spatialPartitionedRDD);
                
            //flatMapToPair, use HashSet.

            JavaPairRDD<Envelope, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByRectangleJudgement());
            
            JavaPairRDD<Envelope, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByRectangle(joinResultWithDuplicates);
            
            JavaPairRDD<Envelope, HashSet<Point>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Point>>()
            {
				@Override
				public HashSet<Point> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Point)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            	
            });
            return castedResult;
        }
    }
        
    
    
    //Function for Spatial Join Query between a RectangleRDD and a PointRDD using Cartesian Product
    public static JavaPairRDD<Envelope, HashSet<Point>> SpatialJoinQueryWithCartesianProduct(SparkContext sc,PointRDD pointRDD, RectangleRDD rectanglequeryRDD, boolean Index) throws Exception {
    
    	
   	 JavaPairRDD<Envelope,HashSet<Point>> res=null;
    
   	
   	 try{
    	 
   		 
   		 //using Rectangle query to create all rectangles
   		 int j;
    	 List<Object> datalist ;
    	 datalist  =rectanglequeryRDD.rawSpatialRDD.collect();
    	
    	 
    	 //List of Envelopes
    	 List<Envelope> envelopes ;
    	 envelopes= new ArrayList<Envelope>();
    	 
    	
    	 
    	 
    	 //Iterating through loop to fill envelopes list 
    	 j=0;
    	 while(j<datalist.size())
    	 {
    	
    		 envelopes.add((Envelope)datalist.get(j));
    		
    		 j++;
    	 
    	 }
    	 
    	 
    	 //list of tuples
    	 List<Tuple2<Envelope,HashSet<Point>>> tupleslist ;
    	 tupleslist= new ArrayList<Tuple2<Envelope,HashSet<Point>>>();
    	 
    	 
    	 //Iterating for each envelope
    	 for (int i=0 ; i< envelopes.size() ; i++)
    	 {
    		 //Each envelope
    		 Envelope env = envelopes.get(i);
    		 
    		 //List of points
    		 List<Point> lpoints; 
    		 lpoints= RangeQuery.SpatialRangeQuery(pointRDD, env, 0, Index).collect();
    		 
    		 //set of points
    		 HashSet<Point> pointSet;
    		 pointSet=new HashSet<>(lpoints);
    		 
    		 //Tuple of Envelope and set of points
    		 Tuple2<Envelope,HashSet<Point>> tuple;
    		 tuple=new Tuple2<>( env, pointSet);
    		 
    		 //Adding the created to the tuples list for each iteration
    		 tupleslist.add(tuple);    	 
    	 }
    	 
    	 //Spark Context 
    	 JavaSparkContext javasc= new JavaSparkContext(sc);

    	 
    	 JavaRDD<Tuple2<Envelope, HashSet<Point>>> temp ;
    	 temp=  javasc.parallelize(tupleslist);
    	 
    	 res= JavaPairRDD.fromJavaRDD(temp);
    	 
    	 //closing the context
    	 //javasc.close();
    
   	 
   	 }
     
   	
   	 
   	 catch(Exception e)
     {
    
   		 e.printStackTrace();
    	
    	 
     }
     
	
   	 return res;
    	
    }
    


    
    

    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Envelope, HashSet<Envelope>> SpatialJoinQuery(RectangleRDD spatialRDD,RectangleRDD queryRDD,boolean useIndex) throws Exception {

        if(useIndex)
        {
        	//Check if rawPointRDD have index.
            if(spatialRDD.indexedRDD == null) {
	            throw new Exception("[JoinQuery][SpatialJoinQuery] Index doesn't exist. Please build index.");
            }
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.indexedRDD.cogroup(queryRDD.spatialPartitionedRDD);

            //flatMapToPair, use HashSet.

            JavaPairRDD<Envelope, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new AllByRectangleJudgementUsingIndex());
            
            JavaPairRDD<Envelope, HashSet<Envelope>> castJoinResultWithDuplicates = joinResultWithDuplicates.mapToPair(new PairFunction<Tuple2<Envelope,HashSet<Geometry>>, Envelope, HashSet<Envelope>>()
            {

				@Override
				public Tuple2<Envelope, HashSet<Envelope>> call(Tuple2<Envelope, HashSet<Geometry>> spatialObjects)
						throws Exception {
					HashSet<Envelope> castSpatialObjects = new HashSet<Envelope>();
					Iterator objectIterator = spatialObjects._2().iterator();
					while(objectIterator.hasNext())
					{
						Envelope spatialObject= (Envelope)objectIterator.next();
						/*
						Envelope castSpatialObject = spatialObject.getEnvelopeInternal();
						if( (spatialObject).getUserData()!=null)
						{
							castSpatialObject.setUserData(spatialObject.getUserData());
						}
						*/
						castSpatialObjects.add(spatialObject);
					}
					return new Tuple2<Envelope,HashSet<Envelope>>(spatialObjects._1(),castSpatialObjects);
				}
            	
            });
            
            JavaPairRDD<Envelope, HashSet<Envelope>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesRectangleByRectangle(castJoinResultWithDuplicates);
            
            return joinListResultAfterAggregation;
        }
        else
        {
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.spatialPartitionedRDD.cogroup(queryRDD.spatialPartitionedRDD);
                
            //flatMapToPair, use HashSet.

            JavaPairRDD<Envelope, HashSet<Envelope>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new RectangleByRectangleJudgement(spatialRDD.grids.size()));
            
            JavaPairRDD<Envelope, HashSet<Envelope>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesRectangleByRectangle(joinResultWithDuplicates);

            return joinListResultAfterAggregation;
        }
    }
   
    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, HashSet<Point>> SpatialJoinQuery(PointRDD spatialRDD,PolygonRDD queryRDD, boolean useIndex) throws Exception {
        if(useIndex)
        {
        	//Check if rawPointRDD have index.
            if(spatialRDD.indexedRDD == null) {
	            throw new Exception("[JoinQuery][SpatialJoinQuery] Index doesn't exist. Please build index.");
            }
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.indexedRDD.cogroup(queryRDD.spatialPartitionedRDD);

            //flatMapToPair, use HashSet.

            JavaPairRDD<Polygon, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgementUsingIndex());
            JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates);
            
            JavaPairRDD<Polygon, HashSet<Point>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Point>>()
            {
				@Override
				public HashSet<Point> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Point)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            });
            return castedResult;
        }
        else
        {
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.spatialPartitionedRDD.cogroup(queryRDD.spatialPartitionedRDD);
                
            //flatMapToPair, use HashSet.

            JavaPairRDD<Polygon, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgement());
            
            JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates);
            
            JavaPairRDD<Polygon, HashSet<Point>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Point>>()
            {
				@Override
				public HashSet<Point> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Point)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            });
            return castedResult;
        }

   }
   

   
    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, HashSet<Polygon>> SpatialJoinQuery(PolygonRDD spatialRDD,PolygonRDD queryRDD, boolean useIndex) throws Exception {
        if(useIndex)
        {
        	//Check if rawPointRDD have index.
            if(spatialRDD.indexedRDD == null) {
	            throw new Exception("[JoinQuery][SpatialJoinQuery] Index doesn't exist. Please build index.");
            }
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.indexedRDD.cogroup(queryRDD.spatialPartitionedRDD);

            //flatMapToPair, use HashSet.

            JavaPairRDD<Polygon, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgementUsingIndex());
            JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates);
            
            JavaPairRDD<Polygon, HashSet<Polygon>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Polygon>>()
            {
				@Override
				public HashSet<Polygon> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Polygon> castedSpatialObjects = new HashSet<Polygon>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Polygon)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            });
            return castedResult;
        }
        else
        {
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.spatialPartitionedRDD.cogroup(queryRDD.spatialPartitionedRDD);
                
            //flatMapToPair, use HashSet.

            JavaPairRDD<Polygon, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgement());
            
            JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates);
            
            JavaPairRDD<Polygon, HashSet<Polygon>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Polygon>>()
            {
				@Override
				public HashSet<Polygon> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Polygon> castedSpatialObjects = new HashSet<Polygon>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Polygon)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            });
            return castedResult;
        }

   }

    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, HashSet<LineString>> SpatialJoinQuery(LineStringRDD spatialRDD,PolygonRDD queryRDD, boolean useIndex) throws Exception {
        if(useIndex)
        {
        	//Check if rawPointRDD have index.
            if(spatialRDD.indexedRDD == null) {
	            throw new Exception("[JoinQuery][SpatialJoinQuery] Index doesn't exist. Please build index.");
            }
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.indexedRDD.cogroup(queryRDD.spatialPartitionedRDD);

            //flatMapToPair, use HashSet.

            JavaPairRDD<Polygon, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgementUsingIndex());
            JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates);
            
            JavaPairRDD<Polygon, HashSet<LineString>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<LineString>>()
            {
				@Override
				public HashSet<LineString> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<LineString> castedSpatialObjects = new HashSet<LineString>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((LineString)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            });
            return castedResult;
        }
        else
        {
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.spatialPartitionedRDD.cogroup(queryRDD.spatialPartitionedRDD);
                
            //flatMapToPair, use HashSet.

            JavaPairRDD<Polygon, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgement());
            
            JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates);
            
            JavaPairRDD<Polygon, HashSet<LineString>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<LineString>>()
            {
				@Override
				public HashSet<LineString> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<LineString> castedSpatialObjects = new HashSet<LineString>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((LineString)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            });
            return castedResult;
        }

   }

    /**
     * Spatial join query count by key.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, Long> SpatialJoinQueryCountByKey(SpatialRDD spatialRDD,PolygonRDD queryRDD, boolean useIndex) throws Exception {
        if(useIndex)
        {
        	//Check if rawPointRDD have index.
            if(spatialRDD.indexedRDD == null) {
	            throw new Exception("[JoinQuery][SpatialJoinQuery] Index doesn't exist. Please build index.");
            }
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.indexedRDD.cogroup(queryRDD.spatialPartitionedRDD);

            //flatMapToPair, use HashSet.

            JavaPairRDD<Polygon, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgementUsingIndex());
            JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates);
            
            JavaPairRDD<Polygon, Long> resultCountByKey = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,Long>()
            {
				@Override
				public Long call(HashSet<Geometry> spatialObjects) throws Exception {

					return (long) spatialObjects.size();
				}
            });
            return resultCountByKey;
        }
        else
        {
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.spatialPartitionedRDD.cogroup(queryRDD.spatialPartitionedRDD);
                
            //flatMapToPair, use HashSet.

            JavaPairRDD<Polygon, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgement());
            
            JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates);
            
            JavaPairRDD<Polygon, Long> resultCountByKey = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,Long>()
            {
				@Override
				public Long call(HashSet<Geometry> spatialObjects) throws Exception {

					return (long) spatialObjects.size();
				}
            });
            return resultCountByKey;
        }

   }
    
    
    /**
     * Spatial join query count by key.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Envelope, Long> SpatialJoinQueryCountByKey(SpatialRDD spatialRDD,RectangleRDD queryRDD,boolean useIndex) throws Exception {

        if(useIndex)
        {
        	//Check if rawPointRDD have index.
            if(spatialRDD.indexedRDD == null) {
	            throw new Exception("[JoinQuery][SpatialJoinQuery] Index doesn't exist. Please build index.");
            }
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.indexedRDD.cogroup(queryRDD.spatialPartitionedRDD);

            //flatMapToPair, use HashSet.

            JavaPairRDD<Envelope, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new AllByRectangleJudgementUsingIndex());
            
            JavaPairRDD<Envelope, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByRectangle(joinResultWithDuplicates);
            
            JavaPairRDD<Envelope, Long> resultCountByKey = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,Long>()
            {
				@Override
				public Long call(HashSet<Geometry> spatialObjects) throws Exception {
					return (long) spatialObjects.size();
				}
            	
            });
            return resultCountByKey;
        }
        else
        {
            if(spatialRDD.spatialPartitionedRDD == null) {
                throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
            }
            else if(queryRDD.spatialPartitionedRDD == null)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
            }
            else if(queryRDD.grids.equals(spatialRDD.grids)==false)
            {
                throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
            }
            JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.spatialPartitionedRDD.cogroup(queryRDD.spatialPartitionedRDD);
                
            //flatMapToPair, use HashSet.

            JavaPairRDD<Envelope, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByRectangleJudgement());
            
            JavaPairRDD<Envelope, HashSet<Geometry>> joinListResultAfterAggregation = DuplicatesHandler.removeDuplicatesGeometryByRectangle(joinResultWithDuplicates);
            
            JavaPairRDD<Envelope, Long> resultCountByKey = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,Long>()
            {
				@Override
				public Long call(HashSet<Geometry> spatialObjects) throws Exception {
					return (long) spatialObjects.size();
				}
            });
            return resultCountByKey;
        }
    }
}
