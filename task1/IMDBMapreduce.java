// import required packages
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.*;

// define the main class name IMDBMapreduce
public class IMDBMapreduce {
	// define the mapper class which takes Object, Text, Text, and IntWritable types as input
	public static class TokenizerMapper1 extends Mapper<Object, Text, Text, IntWritable> {
		// define variables which is used later to write the output
		private final static IntWritable countone = new IntWritable(1);
		private Text movie = new Text();
		// Map function
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Retrieve the filename and input string
			Path filePath = ((FileSplit) context.getInputSplit()).getPath();
			String filename = filePath.getName();
			String inputString = value.toString();
			// Split the input string into an array of movie details
			String[] movieList = inputString.split(";");
			int list_length = movieList.length;
			if (list_length == 5) {
				int n = movieList.length;
				String year = movieList[n - 2];
				String type = movieList[n - 4];
				//variables to keep track of the combinations of genres found in the movie.
				int first_comb = 0;
				int second_comb = 0;
				int third_comb = 0;
				// Loop through years from 2000 to 2020 in increments of 7
				for (int i = 2000; i <= 2020; i += 7) {
					// Check if the year and type are not null and the type is a movie
					if (year != null && !year.equals("\\N") && type != null && !type.equals("\\N") && type.equals("movie")) {
						int end_year = i + 6;
						first_comb = 0;
						second_comb = 0;
						third_comb = 0;
						// Check if the movie year falls within the current year range
						if (Integer.parseInt(year) >= i && Integer.parseInt(year) <= end_year) {
							String genre_comb = movieList[n - 1];
							String genre_names[] = genre_comb.split(",");
							List<String> genre = new ArrayList<>(Arrays.asList(genre_names));
							// Check if the genres list is not null, not empty, and does not contain null values
							if (genre != null && !genre.isEmpty() && !genre.equals("\\N")){
								// Check if the current movie has the required genre combinations and increment the corresponding counters
								if (genre.contains("Comedy") && genre.contains("Romance")) {
									first_comb++;
									}
								if (genre.contains("Action") && genre.contains("Drama")) {
										second_comb++;
									}
								if (genre.contains("Adventure") && genre.contains("Sci-Fi")) {
										third_comb++;
									}
									}
							// Write the output for the required genre combinations and the year range
							if(first_comb == 1) {
								//output format : [2000-2006],Comedy;Romance,<numberOfMovies>
								String mapper1 = String.format("[%d-%d],Comedy;Romance,", i, end_year);
								movie.set(mapper1);
								context.write(movie, countone);
							}
							if(second_comb == 1) {
								String mapper2 = String.format("[%d-%d],Action;Drama,", i, end_year);
								movie.set(mapper2);
								context.write(movie, countone);
							}
							if(third_comb ==1) {
								String mapper3 = String.format("[%d-%d],Adventure;Sci-Fi,", i, end_year);
								movie.set(mapper3);
								context.write(movie, countone);
							}
						}
					}
				}

			}
		}
	}

	// Reduce function
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// Create an instance of IntWritable to hold the sum of movies for each key
		private IntWritable movieList = new IntWritable();

		// reduce method takes in a key-value pair and aggregates the values with the same key
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// Initialize a variable to hold the sum of values
			int sum_values = 0;
			// Iterate through the values and add them up
			Iterator<IntWritable> iter = values.iterator();
			while(iter.hasNext()) {
				IntWritable val = iter.next();
				sum_values += val.get();
			}
			// Set the sum as the value of the IntWritable instance
			movieList.set(sum_values);
			// Write the output key-value pair to the context
			context.write(key, movieList);
		}
	}

		// The main function of the program.
	public static void main(String[] args) throws Exception {
		// Create a new configuration object to hold the job configuration.
		Configuration conf = new Configuration();
		// Create a new job object with a name "movie count".
		Job mapred_job = Job.getInstance(conf, "movie count");
		// Set the main class that will be used to run the job.
		mapred_job.setJarByClass(IMDBMapreduce.class);
		// Set the mapper class that will be used to process the input data.
		mapred_job.setMapperClass(TokenizerMapper1.class);
		// Set the combiner class that will be used to perform local aggregation before shuffling.
		mapred_job.setCombinerClass(IntSumReducer.class);
		// Set the reducer class that will be used to perform the final aggregation of the output data.
		mapred_job.setReducerClass(IntSumReducer.class);
		// Set the output key class for the job.
		mapred_job.setOutputKeyClass(Text.class);
		// Set the output value class for the job.
		mapred_job.setOutputValueClass(IntWritable.class);
		// Set the input and output paths for the job
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		FileInputFormat.addInputPath(mapred_job, inputPath);
		FileOutputFormat.setOutputPath(mapred_job, outputPath);
		// Submit the job to the cluster and wait for it to complete
		if (mapred_job.waitForCompletion(true)) {
			// Job completed successfully
			System.out.println("Job completed successfully");
			System.exit(0);
		} else {
			// Job failed
			System.out.println("Job failed");
			System.exit(1);
		}
	}

}
