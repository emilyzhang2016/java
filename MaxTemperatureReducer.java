import java.io.IOException;

import org.apache.hadoop.io.IntWritable; //相当于java的Integer
import org.apache.hadoop.io.Text;//相当于java的String
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempertureReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key,Iterable<IntWritable> values,Context context)
		throws IOException, InterruptedException{
			int maxValue = Integer.MIN_VALUE;//Integer.MIN_VALUE，即-2147483648，二进制位如下： 
										 //1000 0000 0000 0000 0000 0000 0000 0000 
			for (IntWritable value : values){//values是可迭代类型 因此可以使用foreach语句迭代访问每一个元素
				maxValue = Math.max(maxValue,value.get());
			} 	
			context.write(key, new IntWritable(maxValue));
		}
}
