package ca.ualberta.ssrg.parkmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ca.ualberta.ssrg.parkmeans.model.DistanceMeasurer;

public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final Log LOG = LogFactory.getLog(KMeansMapper.class);
	List<List<Double>> centers = new LinkedList<List<Double>>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));

		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(centroids)));

		String line;
		while ((line = br.readLine()) != null) {
			List<Double> center = fromStringToVector(line);
			centers.add(center);
		}

		br.close();
	}

	@Override
	// protected void map(ClusterCenter key, Vector value, Context context)
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		List<Double> nearestCenter = null;
		double nearestDistance = Double.MAX_VALUE;

		String val = value.toString();
		String[] valParts = val.split(":");
		String vectorString = valParts[1];
		List<Double> vector = fromStringToVector(vectorString);

		for (List<Double> c : centers) {
			double dist = DistanceMeasurer.measureDistance(c, vector);

			if (nearestCenter == null) {
				nearestCenter = c;
				nearestDistance = dist;
			} else {
				if (nearestDistance > dist) {
					nearestCenter = c;
					nearestDistance = dist;
				}
			}
		}
		LOG.info("MAP WRITE " + fromVectorToString(nearestCenter) + " --- " + vectorString);
		context.write(new Text(fromVectorToString(nearestCenter)), new Text(
				vectorString));
	}

	public static List<Double> fromStringToVector(String vectorStr) {
		List<Double> vector = new LinkedList<Double>();
		String[] vals = vectorStr.split(",");

		for (int i = 0; i < vals.length; i++) {
			vector.add(new Double(vals[i]));
		}

		return vector;
	}

	public static String fromVectorToString(List<Double> vector) {
		StringBuffer vectorStr = new StringBuffer();

		for (int i = 0; i < vector.size(); i++) {
			vectorStr.append(vector.get(i));

			if (i < vector.size() - 1) {
				vectorStr.append(",");
			}
		}

		return vectorStr.toString();
	}
}
