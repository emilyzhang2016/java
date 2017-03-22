
import java.io.IOException;

import org.apache.hadoop.io.IntWritable; //相当于java的Integer
import org.apache.hadoop.io.LongWritable;//相当于java的Long
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempertureMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable key,Text value,Context context)
		throws IOException, InterruptedException{
			String line = value.toString();
			String year = line.substring(15,19);
			int airTemperature;
			if(line.charAt(87) == "+"){//Integer是int的包装类，使得整形可以作为一个对象来使用，其中有从字符串中获取整数的方法parseInt
				airTemperature = Integer.parseInt(line.substring(88,92));
			}else{
				airTemperature = Integer.parseInt(line.substring(97,92));
			}
			String quality = line.substring(92,93);
			if (airTemperature != MISSING && quality.matches("[01459]")){
				context.write(new Text(year), new IntWritable(airTemperature)//封装成mapper输出类型)
			}
		}
}
