package org.datasyslab.geospark.PhaseThree;


public class SpatialWeight {
	
	//  w(i,j) - spatial weight between cell i and j 
	
	//spatialWeigthSum - Summation of w(i,j) from j = 1 to n ,  n is equal to the total number of cells
	private double spatialWeigthSum; 
	
	// spatialWeightWithAttributeValueSum  - Summation of w(i,j) * x(j) , x(j) is the attribute value of the cell 
	private double spatialWeightWithAttributeValueSum;

	public SpatialWeight(double spatialWeightSum, double spatialWeightWithAttributeValueSum){
		this.spatialWeigthSum = spatialWeightSum;
		this.spatialWeightWithAttributeValueSum = spatialWeightWithAttributeValueSum;
	}
	
	
	public double getSpatialWeigthSum() {
		return spatialWeigthSum;
	}

	public void setSpatialWeigthSum(double spatialWeigthSum) {
		this.spatialWeigthSum = spatialWeigthSum;
	}

	public double getSpatialWeightWithAttributeValueSum() {
		return spatialWeightWithAttributeValueSum;
	}

	public void setSpatialWeightWithAttributeValueSum(double spatialWeightWithAttributeValueSum) {
		this.spatialWeightWithAttributeValueSum = spatialWeightWithAttributeValueSum;
	}  
	
	
	public String toString(){
		return "spatialWeigthSum:" + spatialWeigthSum + "spatialWeightWithAttributeValueSum:" + spatialWeightWithAttributeValueSum;
	}
	
	

}
