package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class EdgeAvgLen1 {
	//in this part we need to create a class pair to contain values because we need to record the  node length and count number
	//First we need to contact the Writable  and new class pair as a new Writable type
	public static class Pair implements Writable{
		private double sum;
		private int count;
		public Pair(){}
		public Pair(double sum,int count){
			set(sum,count);
		}
		public void set(double left,int right){
			sum = left;
			count = right;
		}
		public double getSum(){
			return sum;
		
		}
		public int getCount(){
			return count;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			sum = in.readDouble();
			count = in.readInt();
		}
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeDouble(sum);
			out.writeInt(count);
		}
		
	}
	public static class mapper extends Mapper<Object, Text, IntWritable, Pair>{
		//in this class  we just record the data no need to sum
		//First we read the input file and the data recorded line by line and then we use split to remove the blank between each number
		//Then we get a string list contains all the number of one line and the 2nd element is the node the 3rd element is the length
		//We need to change the node of text type to integer because this can makes node rank automatically
		//Finally we set the length and count into pair and write the key and pair into context
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] total_list =value.toString().split("\\n");
			for (int i =0;i<total_list.length;i++){
				String[] line_list = total_list[i].split("\\s+");
				Pair p = new Pair(Double.parseDouble(line_list[3]),1);
				IntWritable bb = new IntWritable();
				bb.set(Integer.parseInt(line_list[2]));
				context.write(bb, p);	
		}
	}
	}
	//combiner class is to deal with the sum up so that we do not need to do this in the reducer
	//In this part we get the values of one key and sum them up,and accumulate the count
	//Then put the new pair with new sum and count and key into context
	public static class combiner extends Reducer<IntWritable,Pair,IntWritable,Pair>{
		protected void reduce(IntWritable key,Iterable<Pair> value, Context context) throws IOException,InterruptedException{
			double sum =0;
			int count = 0;
			while(value.iterator().hasNext()){
				Pair p = value.iterator().next();
				sum += p.getSum();
				count += p.getCount();	
			}
			context.write(key, new Pair(sum,count));
		}
	}
	//In the reducer we get the treated data and then avoid missing data we also need to check if the same key we also need to sum it up
	//The main process of reducer is to get the average length and write it into context
	public static class reducer extends Reducer<IntWritable , Pair,IntWritable,DoubleWritable>{
		private DoubleWritable result = new DoubleWritable();
		protected void reduce(IntWritable key,Iterable<Pair> value,Context context) throws IOException,InterruptedException{
			double  sum = 0;
			int count =0;
			while (value.iterator().hasNext()){
				Pair p = value.iterator().next();
				sum += p.getSum();
				count += p.getCount();
			}
			double aver = sum/count;
			result.set(aver);
			context.write(key, result);
		}
	}
	
		public  static void main(String[] args) throws Exception {
			   Configuration conf = new Configuration();
			    Job job = Job.getInstance(conf, "EdgeAvgLen1");
			    job.setJarByClass(EdgeAvgLen1.class);
			    job.setMapperClass(mapper.class);
			    job.setCombinerClass(combiner.class);
			    job.setReducerClass(reducer.class);
			    job.setOutputKeyClass(IntWritable.class);
			    job.setOutputValueClass(DoubleWritable.class);
			    job.setMapOutputKeyClass(IntWritable.class);
			    job.setMapOutputValueClass(Pair.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

	
