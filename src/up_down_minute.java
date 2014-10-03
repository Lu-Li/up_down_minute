/*-----------------------
2014-08-28
function: track the total up and down flow of each LAC per minute
-------------------------*/
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class up_down_minute {

	public static class Map extends Mapper<Object, Text, Text, Text>
	{
		
		private Text keyInfo = new Text();
		private Text ValueInfo = new Text();
		private String hour1=new String();
		private String minute1=new String();
		private String hour2=new String();
		private String minute2=new String();
		
		private static String type="913";
		private static int type_length=type.length();
		
		private static String LAC="33598";
		private static int LAC_length=LAC.length();
		
		private static String start_time="2012-11-16 23:55:37.7457210";
		private static int start_time_length=start_time.length();

		public static int isLegal(String[] str){
			
			int isLegal=1;
			
			if (str[1].length()!=LAC_length||str[4].length()!=type_length||str[5].length()!=start_time_length){
				isLegal=-1;
			}
			
			return isLegal;
		}                       
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException
			{	
				String forstring=value.toString();
				String forString[]=forstring.split("[|]");
				if (forString.length<26) {
					return;
				}
				if (isLegal(forString)!=1) {
				return;
			}                                                       
			else{
				hour1=forString[5].substring(11,13);
				minute1=forString[5].substring(14,16);
				hour2=forString[6].substring(11,13);
				minute2=forString[6].substring(14,16);

				int sum = Integer.parseInt(minute2)-Integer.parseInt(minute1)+60*((24+Integer.parseInt(hour2)-Integer.parseInt(hour1))%24);
				if (sum==0)
				{
					keyInfo.set(forString[1]+"\t"+hour1+":"+minute1);
					ValueInfo.set(forString[8]+"\t"+forString[9]+"\t"+forString[10]);
					context.write(keyInfo,ValueInfo);
				}
				else
				{
					ValueInfo.set(Long.toString(Long.parseLong(forString[8])/sum)+"\t"+Long.toString(Long.parseLong(forString[9])/sum)+"\t"+Long.toString(Long.parseLong(forString[10])/sum));       
					for(int n=0;n<sum;n++)
					{
						int minute = Integer.parseInt(minute1)+n;
						if(minute>=60)
						{
							minute = minute % 60;
							int hour = Integer.parseInt(hour1) + 1;
							if(hour > 23)
							{
								return;
							}
							if (hour<10)
							{
								hour1="0"+Integer.toString(hour);
							}
						}
						if (minute<10)
						{
							minute1="0"+Integer.toString(minute);
						}
						
						keyInfo.set(forString[4]+"\t"+hour1+":"+minute1);
						context.write(keyInfo,ValueInfo);					
					}
				 }
				
				}
				
			}
	}
		
	public static class IntSumReducer extends
	Reducer<Text, Text, Text, Text>
	{
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException
{
	long sum1=0,sum2=0,sum3 = 0;
	for (Text val : values)
	{
		String forstring=val.toString();
		String forString[]=forstring.split("[\t]");
		sum1 += Long.parseLong(forString[0]);
		sum2 += Long.parseLong(forString[1]);
		sum3 += Long.parseLong(forString[2]);
	}
	result.set(Long.toString(sum1)+"\t"+Long.toString(sum2)+"\t"+Long.toString(sum3));
	context.write(key, result);
}
}
	
		

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();

		Job job = new Job(conf, "InvertedIndex");
		job.setJarByClass(up_down_minute.class);
		
		job.setMapperClass(Map.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
			
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
