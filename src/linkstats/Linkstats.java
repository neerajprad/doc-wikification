package linkstats;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Linkstats {
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		private String delim = "<>";

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Text mapper_key = new Text();
			Text mapper_value = new Text();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, delim);
			mapper_value.set(tokenizer.nextToken());
			mapper_key.set(tokenizer.nextToken());
			context.write(mapper_key, mapper_value);
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		private String delim = "\t";

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Integer> entityCnts = new HashMap<String, Integer>();
			Text reducer_output = new Text();
			Integer tot_entityCnts = 0;
			for (Text val : values) {
				tot_entityCnts++;
				String entity = val.toString();
				if (entityCnts.containsKey(entity)) {
					entityCnts.put(entity, entityCnts.get(entity) + 1);
				} else {
					entityCnts.put(entity, 1);
				}
			}
			String reducer_value = delim + tot_entityCnts.toString();
			for (Map.Entry<String, Integer> entry : entityCnts.entrySet()) {
				reducer_value = reducer_value + delim + entry.getKey() + delim
						+ entry.getValue().toString();
			}
			reducer_output.set(reducer_value);
			context.write(key, reducer_output);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "linkstats");
		job.setJarByClass(Linkstats.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
