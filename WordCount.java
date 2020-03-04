import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, Text> {

		//private final static IntWritable one = new IntWritable(1);
		private static Text hadoopText = new Text();
		private static Text hadoopTextValue = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			//[word][filename;byteoffset]
			
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			String line = value.toString().replaceAll("[^a-zA-Z0-9_-]", " ");
			
			StringTokenizer lineTokenizer = new StringTokenizer(line);

			while (lineTokenizer.hasMoreTokens()) {
				String word = lineTokenizer.nextToken();
				word = word.toLowerCase();
				
				if (word.length() > 1) {
					hadoopText.set(word);
					hadoopTextValue.set(fileName + ";" + key);
					context.write(hadoopText, hadoopTextValue);	
					//context.write(hadoopText, one);					
				}
			}
		}
	}

	public static class IntSumReducer
	//extends Reducer<Text,IntWritable,Text,IntWritable> {
	extends Reducer<Text, Text, Text, Text> {
		
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			
			String word = key.toString();
			Hashtable<String, ArrayList<Long>> wordOccurrencesInFiles 
				= new Hashtable<String, ArrayList<Long>>();
			
			for (Text value : values) {
				
				String[] fileNameAndLineOffset = value.toString().split(";");
				String fileName = fileNameAndLineOffset[0];
				Long lineOffset = Long.parseLong(fileNameAndLineOffset[1]);
								
				if (!wordOccurrencesInFiles.containsKey(fileName)) {
					ArrayList<Long> lineOffsets = new ArrayList<Long>();
					wordOccurrencesInFiles.put(fileName, lineOffsets);
				}
				
				wordOccurrencesInFiles.get(fileName).add(lineOffset);
				
			}
			
			StringBuilder reducedValueBuilder = new StringBuilder();			
			reducedValueBuilder.append(word + "{\n");
			
			wordOccurrencesInFiles.forEach((file, offsets) -> { 
				reducedValueBuilder.append(file + " { "); 
	            
				for (long offset : offsets) {
					reducedValueBuilder.append(offset + ", ");
				}
				
	            reducedValueBuilder.append("},");
	        });
			
			reducedValueBuilder.append("},\n");
			
			String reducedValue = reducedValueBuilder.toString();
			result.set(reducedValue);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}