package comp9313.ass2;

//import ...
import java.io.IOException;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleTargetSP {


    public static String OUT = "output";
    public static String IN = "input";

    public static class STMapper extends Mapper<Object, Text, LongWritable, Text> {

        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // YOUR JOB: map function
            // ... ...
        }

    }


    public static class STReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // YOUR JOB: reduce function
            // ... ...
        }
    }


    public static void main(String[] args) throws Exception {        

        IN = args[0];

        OUT = args[1];

        int iteration = 0;

        String input = IN;

        String output = OUT + iteration;

	    // YOUR JOB: Convert the input file to the desired format for iteration, i.e., 
        //           create the adjacency list and initialize the distances
        // ... ...

        boolean isdone = false;

        while (isdone == false) {

            // YOUR JOB: Configure and run the MapReduce job
            // ... ...                   
            
            input = output;           

            iteration ++;

            output = OUT + iteration;

            //You can consider to delete the output folder in the previous iteration to save disk space.

            // YOUR JOB: Check the termination criterion by utilizing the counter
            // ... ...

            if(the termination condition is reached){
                isdone = true;
            }
        }

        // YOUR JOB: Extract the final result using another MapReduce job with only 1 reducer, and store the results in HDFS
        // ... ...
    }

}

