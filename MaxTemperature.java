
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature{
	public static void main(String[] args throws Exception){
		if(args.length != 2){
			System.err.printIn("Usage: MaxTemperature <input path> <output path>");
			system.exit(-1);
		}
		Job job =new Job();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max Temperature");

		FileInputFormat.addInputPath(job, new path(args[0]));
		FileOutputFormat.setOutputPath(job, new path(args[1]));

		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		system.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
