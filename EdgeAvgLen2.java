package comp9313.ass1;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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




public class EdgeAvgLen2 {
	//in this part we need to create a class pair to contain values because we need to record the  node's length and count number
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
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeDouble(sum);
			out.writeInt(count);
		}
		
	}
	
	public static class mapper extends Mapper<Object, Text, IntWritable, Pair>{
		//In the class we need to new a map which like a dictionary to  contain the key and value 
		//And we also need to add a judgment statement to finish the sum up If  the map has the current  key we add the current value to the value in the map
		//If the map does not  have this key we just put the length and count (1) into map
		//In this part we finish the sum up 
		private Map<Integer,Pair> map = new HashMap<Integer,Pair>();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] total_list =value.toString().split("\\n");
			String[] line_list;
			for (int i =0;i<total_list.length;i++){
				line_list = total_list[i].split("\\s+");
				int tem_in = Integer.parseInt(line_list[2]);
			if(!map.containsKey(tem_in)){
				Pair pa = new Pair();
				pa.set(Double.parseDouble(line_list[3]), 1);
				map.put(tem_in,pa);
				
			}else{
				Pair tmp = map.get(tem_in);
				double ss = tmp.getSum();
				int cc = tmp.getCount();
				cc = cc +1;
				double sa = Double.parseDouble(line_list[3])+ss;
				Pair pair = new Pair ();
				pair.set(sa, cc);
				map.put(tem_in,pair);
			
			}
			
		}
		}
	//In cleanup we need to get all the key and value from the map and write into context
	protected void cleanup(Context context)throws IOException,InterruptedException{
		for(Map.Entry<Integer, Pair>e : map.entrySet()){
			int tt = e.getKey();
			Pair p = e.getValue();
			context.write(new IntWritable(tt), p);
		}
	}
	}
	//In the reducer we get the key and value from clean up and sum it up then we get the average length  from reducer
	//Write key and  the average length into context
	public static class reducer extends Reducer<IntWritable, Pair, IntWritable, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
		public void reduce(IntWritable key, Iterable<Pair> value, Context context) throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			while (value.iterator().hasNext()) {
				Pair p = value.iterator().next();
				sum += p.getSum();
				count += p.getCount();
		
				}
			double average = sum/count;
				result.set(average);
				context.write(key, result);
		}
	
	}
	//The main class is just to run these classes 
		public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "EdgeAvgLen2");
		    job.setJarByClass(EdgeAvgLen2.class);
		    job.setMapperClass(mapper.class);
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
	
	