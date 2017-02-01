package org.datasyslab.geospark.PhaseThree;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CalculateZScore {
	
	private static final String COMMA = ",";
	private static final String NEWLINE = "\n";
	private static final String HEADER = "cell_x,cell_y,time_step,z_score";

	public List<FinalOutput> calculateZScore(HashMap<CubeId, Integer> inputMap,String outputFile) throws IOException {

		double totalCells = 2200 * 31;

		double attributeValueMean = getAttributeValueMean(inputMap, totalCells);
		// System.out.println("attributeValueMean:"+attributeValueMean);
		double sValue = getSValue(inputMap, totalCells);
		// System.out.println("sValue:"+sValue);

		SpatialWeight sw;

		List<FinalOutput> scores = new ArrayList<FinalOutput>();/*
																 * (int)
																 * totalCells,
																 * new
																 * Comparator
																 * <FinalOutput
																 * >() {
																 * 
																 * @Override
																 * public int
																 * compare
																 * (FinalOutput
																 * o1,
																 * FinalOutput
																 * o2) { // TODO
																 * Auto
																 * -generated
																 * method stub
																 * if(o1.score >
																 * o2.score)
																 * return 1;
																 * if(o1.score <
																 * o1.score)
																 * return -1;
																 * return 0; }
																 * });
																 */

		Iterator it = inputMap.entrySet().iterator();
		while (it.hasNext()) {

			Map.Entry pair = (Map.Entry) it.next();
			CubeId c = (CubeId) pair.getKey();

			sw = getCellSpatialWeightSums(c, inputMap);
			// System.out.println(sw);
			double denominator = getDenominator(sw, totalCells);
			// System.out.println("denominator:" + denominator);
			double zScore = (sw.getSpatialWeightWithAttributeValueSum() - (attributeValueMean * sw
					.getSpatialWeigthSum())) / (sValue * denominator);
			// System.out.println("zScore: "+ zScore);
			// System.out.println(c.x1 + "," + c.y1 + "," + c.x2 + "," + c.y2 +
			// "," + c.day + "," + zScore);
			scores.add(new FinalOutput(c, zScore));
		}

		System.out.println(scores.size());
		Collections.sort(scores, new Comparator<FinalOutput>() {
			public int compare(FinalOutput o1, FinalOutput o2) {
				return Double.valueOf(o2.score).compareTo(
						Double.valueOf(o1.score));
			}
		});
		// Collections.reverse(scores);
		FileWriter fw = new FileWriter(outputFile);
		fw.append(HEADER);
		fw.append(NEWLINE);
		for (int i = 1; i <= 50; i++) {
			FinalOutput f = scores.get(i);
			CubeId c = f.id;
			System.out.println(c.x1 + "," + c.y1 + "," + c.day + "," + f.score);
			fw.append(String.valueOf(c.x1*100));
			fw.append(COMMA);
			fw.append(String.valueOf(c.y1*100));
			fw.append(COMMA);
			fw.append(String.valueOf(c.day));
			fw.append(COMMA);
			fw.append(String.valueOf(f.score));
			fw.append(NEWLINE);
		}
		fw.flush();
		fw.close();
		return scores;
	}

	private Double getSValue(HashMap<CubeId, Integer> inputMap,
			double totalCells) {

		Double sValue = Math.sqrt(getAttributeValueSquaredMean(inputMap,
				totalCells)
				- Math.pow(getAttributeValueMean(inputMap, totalCells), 2));

		return sValue;
	}

	private Double getAttributeValueMean(HashMap<CubeId, Integer> inputMap,
			double totalCells) {

		Double attributeValueSum = 0.0;
		Double attributeValueMean = 0.0;

		for (int d : inputMap.values()) {

			attributeValueSum += d;

		}

		attributeValueMean = attributeValueSum / totalCells;

		return attributeValueMean;

	}

	private Double getAttributeValueSquaredMean(
			HashMap<CubeId, Integer> inputMap, double totalCells) {

		Double attributeValueSum = 0.0;
		Double attributeValueMean = 0.0;

		for (double d : inputMap.values()) {

			attributeValueSum += Math.pow(d, 2);

		}

		attributeValueMean = attributeValueSum / totalCells;

		return attributeValueMean;

	}

	private SpatialWeight getCellSpatialWeightSums(CubeId key,
			HashMap<CubeId, Integer> inputMap) {

		SpatialWeight sw = new SpatialWeight(0.0, 0.0);
		double tempSpatialWeigthSum = 0.0;
		double tempSpatialWeightWithAttributeValueSum = 0.0;

		for (double x = -0.01; x <= 0.01; x += 0.01) {
			for (double y = -0.01; y <= 0.01; y += 0.01) {
				for (int z = -1; z <= 1; ++z) {
					// System.out.println("key:"+key);
					CubeId cube = new CubeId(Integer.valueOf(z
							+ Integer.valueOf(key.day)), x + key.x1, y + key.y1);
					if (isCellInBoundary(cube)) {
						tempSpatialWeigthSum += 1.0;
					}
					// System.out.println("cube:"+cube);
					if (inputMap.containsKey(cube)) {

						// tempSpatialWeigthSumSquared += 1.0;
						tempSpatialWeightWithAttributeValueSum += inputMap
								.get(cube);

					}

				}
			}
		}

		sw.setSpatialWeigthSum(tempSpatialWeigthSum);
		sw.setSpatialWeightWithAttributeValueSum(tempSpatialWeightWithAttributeValueSum);
		return sw;
	}

	private double getDenominator(SpatialWeight sw, double size) {

		double spatialWeigthSum = sw.getSpatialWeigthSum();
		double spatialWeigthSumSquared = sw.getSpatialWeigthSum();

		double value = Math.sqrt((size * spatialWeigthSumSquared - Math.pow(
				spatialWeigthSum, 2)) / (size - 1));

		return value;
	}

	private boolean isCellInBoundary(CubeId c) {
		double x1 = 4050;
		double x2 = 4090;
		double y1 = -7425;
		double y2 = -7370;
		double nx1 = c.x1 * 100;
		double ny1 = c.y1 * 100;
		boolean result = true;
		if (nx1 < x1) {
			result = false;
		} else if (nx1 > x2) {
			result = false;
		} else if (ny1 < y1) {
			result = false;
		} else if (ny1 > y2) {
			result = false;
		} else if (c.day > 30 || c.day < 0)
			result = false;
		return result;
	}

}
