import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class UberActiveVehicles {
	//Extending the mapper default class, key-in is of type long writtable, value in as text, value out as text and 
	//key out as Intwritable 
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("MM/dd/yyyy");
		String[] days ={"Sun","Mon","Tue","Wed","Thu","Fri","Sat"};
		//basement will store the different basement numbers.
		private Text basement = new Text();
		Date date = null;
		private int active_vehicles;
		//overwriting the map method which will run for once every line
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//we are storing the line in a string variable line
			String line = value.toString();
			//we are splitting the line and storing it in a String array so that all the columns in a row are stored
			//in it.
			String[] splits = line.split(",");
			basement.set(splits[0]);
			try {
				date = format.parse(splits[1]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			active_vehicles = new Integer(splits[2]);
			String keys = basement.toString()+ " "+days[date.getDay()];
			//we are entering the key and value into context which will act as the output of the Map reduce method.
			context.write(new Text(keys), new IntWritable(active_vehicles));
		}
}

//Extending the default reducer class and the first Text and IntWritable are the outputs of the map reduce program
//The key out and value out is the second Text and IntWritable
public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	//Overwriting the reduce method which will run for every key
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		//sum will store the sum of all the values for every key	
		int sum = 0;
		//The loop will run for each value in the IntWritable that will come from the shuffle and sort face after the 
		//mapper phase
		for (IntWritable val : values) {
			//storing and calculating the sum of the values
			sum += val.get();
		}
		result.set(sum);
		//write the key and obtained sum as the value.
		context.write(key, result);
	}
}


public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	
	Job job = Job.getInstance(conf, "Uber1");
	
	job.setJarByClass(UberActiveVehicles.class);
	
	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(IntSumReducer.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
}