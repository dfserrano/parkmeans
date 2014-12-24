package ca.ualberta.ssrg.parkmeans;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<Text, Text, LongWritable, Text> {

	public static enum Counter {
		CONVERGED
	}

	private static final Log LOG = LogFactory.getLog(KMeansReducer.class);
	List<String> centers = new LinkedList<String>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<String> vectorList = new LinkedList<String>();

		List<Double> center = KMeansMapper.fromStringToVector(key.toString());
		int vectorSize = center.size();
		List<Double> newCenter = new LinkedList<Double>(Collections.nCopies(
				vectorSize, 0d));

		for (Text value : values) {
			List<Double> vector = KMeansMapper.fromStringToVector(value
					.toString());
			vectorList.add(value.toString());

			for (int i = 0; i < vector.size(); i++) {
				newCenter.set(i, newCenter.get(i) + vector.get(i));
			}
		}

		for (int i = 0; i < newCenter.size(); i++) {
			newCenter.set(i, newCenter.get(i) / vectorList.size());
		}

		String newCenterString = KMeansMapper.fromVectorToString(newCenter);
		centers.add(newCenterString);
		LongWritable lw = new LongWritable(0);
		for (String v : vectorList) {
			LOG.info("REDUCE EMIT " + newCenterString + ":" + v);
			context.write(lw, new Text(newCenterString + ":" + v));
		}

		if (areDifferent(center, newCenter))
			context.getCounter(Counter.CONVERGED).increment(1);

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);

		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("centroid.path"));

		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				fs.create(outPath, true)));

		for (String center : centers) {
			bw.write(center + "\n");
		}
		bw.close();
	}

	public boolean areDifferent(List<Double> a, List<Double> b) {
		if (a.size() != b.size())
			return true;

		for (int i = 0; i < a.size(); i++) {
			if (!a.get(i).equals(b.get(i))) {
				return true;
			}
		}

		return false;
	}
}