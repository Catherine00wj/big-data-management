package comp9313.ass2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//import ...

public class SingleTargetSP {


    public static String OUT = "output";
    public static String IN = "input";
    //public static String Require_node = "0";
    public static enum COUNTER {
        UPDATE
    };
    public static class Pair implements Writable{
		private int out_node;
		private double distance;
		public Pair(){}
		public Pair(int out_node,double distance){
			set(out_node,distance);
		}
		public void set(int left,double right){
			out_node = left;
			distance = right;
		}
		public int getNode(){
			return out_node;
		
		}
		public double getDist(){
			return distance;
		}
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			out_node = in.readInt();
			distance = in.readDouble();
		}
		public void write(DataOutput out) throws IOException {
					// TODO Auto-generated method stub
			out.writeInt(out_node);
			out.writeDouble(distance);
				}	
		}

    public static class STMapper extends Mapper<Object, Text, LongWritable, Pair> {
    	
    	private LongWritable node = new LongWritable(); 
        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            //System.out.println("2222" + tokens[1]);
            if (tokens.length ==4){
            	node.set(Long.parseLong(tokens[2]));
            	int output_node = Integer.parseInt(tokens[1]);
            	double dist = Double.parseDouble(tokens[3]);
            	Pair p = new Pair();
            	p.set(output_node, dist);
            	context.write(node,p);
        }   
    }
    }
    public static class STReducer extends Reducer<LongWritable, Pair, LongWritable, Text> {
    	private Text result = new Text();
        @Override
        public void reduce(LongWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
            LongWritable k = key;
            String k_s = k.toString();
            String  s  = k_s + "|" + " ";
            if (k_s.equals("0")){
            	s = s + "0.0" + " ";
            }else{
            	s = s +  "inf" + " ";
            }
        	for (Pair val: values){
        		String out_node = String.valueOf(val.getNode());
        		String distance = String.valueOf(val.getDist());
        		s = s + out_node + ":" + distance + " ";
        		
            	//System.out.println(val);	
            }
        	s = s + "|INPUT" +  "|ALL";
        	result.set(s);
        	context.write(key, result);
        }
    }
    public static class SSSPMapper extends Mapper<Object, Text, LongWritable, Text> {
    	
    	private LongWritable node_sss = new LongWritable();
        private LongWritable fir_node = new LongWritable();
    	private Text sssresult = new Text();
        private Text fir_result = new Text();
        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\|");
            String[] list = tokens[1].split(" ");
            //System.out.println("2222" + tokens[1]);
            String in_node = tokens[0];
            
            if(list[0].equals("0.0")){
                fir_node.set(Long.parseLong(in_node));
                fir_result.set(in_node +"|"  +  tokens[1] + "|INPUT" + "|ALL");
                context.write(fir_node,fir_result);
            	for(int i =1;i<list.length;i++){
            		String ss = list[i].split(":")[0] +"|" +" ";
            		ss = ss + list[i].split(":")[1] + " " + "|" + in_node+ "|PART";
            		node_sss.set(Long.parseLong(list[i].split(":")[0]));
            		sssresult.set(ss);
            		context.write(node_sss,sssresult);
            	}
            }
           if (list[0].equals("inf")){
               for(int i =1;i<list.length;i++){
                   String ss = in_node +"|" +" ";
                   ss = ss + tokens[1]+ "|INPUT"+ "|ALL";
                   node_sss.set(Long.parseLong(in_node));
                   sssresult.set(ss);
                   context.write(node_sss,sssresult);
           }
           }
            if(!list[0].equals("0.0")&& !list[0].equals("inf")){
                for(int i =1;i<list.length;i++){
                    String ss = list[i].split(":")[0] +"|" +" ";
                    double add_dist = Integer.parseInt(list[i].split(":")[1]) + Integer.parseInt(list[0]);
                    ss = ss + String.valueOf(add_dist) + " " + "|" + in_node+ "|PART";
                    node_sss.set(Long.parseLong(list[i].split(":")[0]));
                    sssresult.set(ss);
                    context.write(node_sss,sssresult);
                }
            }
    }
    }
    public static class SSSPReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        private LongWritable node_sr = new LongWritable();
        //private LongWritable fir_node = new LongWritable();
        private Text result_sr = new Text();
        //private Text fir_result = new Text();
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String node_list = "";
            String total_list = "";
            String current ="inf";
            String least="inf" ;
            String pre_node = "INPUT";
            double min = Double.POSITIVE_INFINITY ;
            for (Text val: values){
            	String tem = val.toString();
                if(tem.indexOf("ALL") != -1){
                    String node_re = val.toString().split("\\|")[1];
                    String[] node_list_sr = node_re.split(" ");
                    current = node_list_sr[0];
                    for(int j= 1 ;j<node_list_sr.length;j++){
                        node_list = node_list + node_list_sr[j] + " ";
                    }
                }
                if(tem.indexOf("PART") !=-1){
                    String node_re = tem.split("\\|")[1];
                    String[] node_list_sr = node_re.split(" ");
                    current = node_list_sr[0];
                    pre_node =node_list_sr[1];
                }
                if (current.equals("inf")){
                    least = "inf";
                }
                if( !current.equals("inf")){
                	if (Double.parseDouble(current) < min){
                		min  = Double.parseDouble(current);
                		least  = String.valueOf(min);
                		context.getCounter(COUNTER.UPDATE).increment(1);;
                	}
            }
            }
            total_list = total_list +String.valueOf(key) + "|" +" " + least + node_list + "|" +pre_node + "|ALL";
            result_sr.set(total_list);
            context.write(key,result_sr);
        }
        }
    
    
    
    
    
    public static void main(String[] args) throws Exception {        
        IN = args[0];
        OUT = args[1];
        //Require_node= args[2];
        int iteration = 0;
        String input = IN;
        String output = OUT + iteration;
        Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "single");
	    job.setJarByClass(SingleTargetSP.class);
	    job.setMapperClass(STMapper.class);
	    job.setReducerClass(STReducer.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Pair.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.waitForCompletion(true);
	    
        boolean isdone = false;

        while (isdone == false) {

            // YOUR JOB: Configure and run the MapReduce job
            // ... ...                   
            
            input = output;           

            iteration ++;

            output = OUT + iteration;
            Job sssjob = Job.getInstance(conf, "SingLESSP");
            sssjob.setJarByClass(SingleTargetSP.class);
            sssjob.setMapperClass(SSSPMapper.class);
            sssjob.setReducerClass(SSSPReducer.class);
            sssjob.setOutputKeyClass(LongWritable.class);
            sssjob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(sssjob, new Path(input));
            FileOutputFormat.setOutputPath(sssjob, new Path(output));

            sssjob.waitForCompletion(true);

            Counters counters = sssjob.getCounters();
            long updates = counters.findCounter(COUNTER.UPDATE).getValue();
            System.out.println("ateUpds: " + updates);

            if (updates == 0){
            	isdone = true;
            }

            //You can consider to delete the output folder in the previous iteration to save disk space.

            // YOUR JOB: Check the termination criterion by utilizing the counter
            // ... ...

            //if(the termination condition is reached){
                //isdone = true;
            //}
        }

        // YOUR JOB: Extract the final result using another MapReduce job with only 1 reducer, and store the results in HDFS
        // ... ...
    }
    

    }
    

