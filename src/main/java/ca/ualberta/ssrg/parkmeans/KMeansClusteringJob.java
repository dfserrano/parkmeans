package ca.ualberta.ssrg.parkmeans;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ca.ualberta.ssrg.parkmeans.model.ClusterCenter;
import ca.ualberta.ssrg.parkmeans.model.Vector;

public class KMeansClusteringJob {

	private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		int iteration = 1;
		LOG.info("ITERATION " + iteration);
		Configuration conf = new Configuration();
		conf.set("num.iteration", iteration + "");

		Path in = new Path("files/clustering/import/data");
		Path center = new Path("files/clustering/import/center/cen.txt");
		conf.set("centroid.path", center.toString());
		Path out = new Path("files/clustering/depth_1");

		Job job = Job.getInstance(conf);
		job.setJobName("KMeans Clustering");

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setJarByClass(KMeansMapper.class);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}
		if (fs.exists(center)) {
			// fs.delete(out, true);
		}
		if (fs.exists(in)) {
			// fs.delete(out, true);
		}

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		long counter = job.getCounters()
				.findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		iteration++;
		while (counter > 0) {
			LOG.info("ITERATION " + iteration);
			conf = new Configuration();
			conf.set("centroid.path", center.toString());
			conf.set("num.iteration", iteration + "");
			job = Job.getInstance(conf);
			job.setJobName("KMeans Clustering " + iteration);

			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansMapper.class);

			in = new Path("files/clustering/depth_" + (iteration - 1) + "/");
			out = new Path("files/clustering/depth_" + iteration);

			if (fs.exists(out))
				fs.delete(out, true);

			FileInputFormat.addInputPath(job, in);
			FileOutputFormat.setOutputPath(job, out);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			job.waitForCompletion(true);
			iteration++;
			counter = job.getCounters()
					.findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		}

		Path result = new Path("files/clustering/depth_" + (iteration - 1)
				+ "/");

		FileStatus[] stati = fs.listStatus(result);
		for (FileStatus status : stati) {
			Path path = status.getPath();
			LOG.info("FOUND " + path.toString());

			if (!status.isDirectory() && path.toString().indexOf("_") == -1) {

				SequenceFile.Reader reader = new SequenceFile.Reader(conf,
						SequenceFile.Reader.file(path));
				ClusterCenter key = new ClusterCenter();
				Vector v = new Vector();
				while (reader.next(key, v)) {
					LOG.info(key + " / " + v);
				}
				reader.close();
			}
		}
	}

}