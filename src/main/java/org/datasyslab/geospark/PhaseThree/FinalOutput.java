package org.datasyslab.geospark.PhaseThree;

import java.util.Comparator;

public class FinalOutput implements Comparator<FinalOutput> {
	CubeId id;
	double score;

	public FinalOutput(CubeId id, double score) {
		this.id = id;
		this.score = score;
	}

	public CubeId getId() {
		return id;
	}

	public void setId(CubeId id) {
		this.id = id;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public int compare(FinalOutput o1,FinalOutput o2) {
		// TODO Auto-generated method stub
		double s1 = o1.getScore();
		double s2 = o2.getScore();

		if(s1 > s2)
			return -1;
		if(s1 < s2)
			return 1;
		return 0;
	}

}
