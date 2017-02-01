package org.datasyslab.geospark.PhaseThree;

import java.io.Serializable;

class CubeId implements Serializable{

	public int day;
	public double x1, y1;

	public CubeId(int day, double x1, double y1)
	{
		this.day = day;
		this.x1 = x1;
		this.y1 = y1;
	}

	public boolean equals(Object o){
		if(o == this){
			return true;
		}
		if(!(o instanceof CubeId)){
			return false;
		}
		CubeId t1 = (CubeId) o;
		return t1.day == day && t1.x1 == x1 && t1.y1 == y1 ;
	}

	public int hashCode(){
		int result = 0;
		result += (int)x1;
		result += (int)y1;
		return result;
	}
	
	public String toString(){
		return day + ":" + x1 + ":" + y1 ;
	}
}
