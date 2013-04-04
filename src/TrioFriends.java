

import java.io.IOException;
import java.util.*; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

public class TrioFriends 
{
	public static class FriendsMapper extends Mapper<Object, Text, Text, IntWritable>
	{
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] friends = value.toString().split(" ");
			String user = friends[0];
			
			for(int i = 1 ; i < friends.length ; i++ )
			{
				for( int j = i+1 ; j < friends.length ; j++ )
				{
					String[] cand = new String[]{user, friends[i], friends[j]};
					Arrays.sort(cand);
					word.set(cand[0]+" "+cand[1]+" "+cand[2]);
					context.write(word, one);
				}
			}
		}
		
	}

	public static class FriendsReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private final static IntWritable one = new IntWritable(1);
		private Text keyword = new Text();
	    
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException 
		{	
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
				if(sum >= 2)
				{
					// These three people are mutual friends with each other, return result
					String[] tokens = key.toString().split(" ");
					keyword.set("<"+tokens[0]+","+tokens[1]+","+tokens[2]+">");
					context.write(keyword, one);
					keyword.set("<"+tokens[1]+","+tokens[0]+","+tokens[2]+">");
					context.write(keyword, one);
					keyword.set("<"+tokens[2]+","+tokens[0]+","+tokens[1]+">");
					context.write(keyword, one);
					return ;
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		//String test = "I love Anni and hope her succeed!";
		
		Configuration conf = new Configuration();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (args.length != 2) 
		{
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "TriFriends");
		//job.setJarByClass(WordCount.class);
		job.setMapperClass(FriendsMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(FriendsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
