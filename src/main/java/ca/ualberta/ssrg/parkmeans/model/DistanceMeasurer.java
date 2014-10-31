package ca.ualberta.ssrg.parkmeans.model;

import java.util.List;

public class DistanceMeasurer {

	/**
	 * Manhattan stuffz.
	 * 
	 * @param center
	 * @param v
	 * @return
	 */
	public static final double measureDistance(ClusterCenter center, Vector v) {
		double sum = 0;
		int length = v.getVector().length;
		for (int i = 0; i < length; i++) {
			sum += Math.abs(center.getCenter().getVector()[i]
					- v.getVector()[i]);
		}

		return sum;
	}

	public static final double measureDistance(List<Double> center,
			List<Double> v) {
		double sum = 0;
		int length = v.size();

		for (int i = 0; i < length; i++) {
			sum += Math.abs(center.get(i) - v.get(i));
		}

		return sum;
	}

}