/**
 *
 */
package org.theseed.metabolism;

/**
 * This is a very simple class that represents a physical location on a metabolic map.
 * It is sorted by the y-coordinate and then the x-coordinate, so that coordinates
 * present in display order.
 *
 * @author Bruce Parrello
 *
 */
public class Coordinate implements Comparable<Coordinate> {

    /** x-coordinate */
    private double x;
    /** y-coordinate */
    private double y;

    /**
     * Construct a specific coordinate.
     *
     * @param xLoc		x-value
     * @param yLoc		y-value
     */
    public Coordinate(double xLoc, double yLoc) {
        this.x = xLoc;
        this.y = yLoc;
    }

    @Override
    public int compareTo(Coordinate o) {
        int retVal = Double.compare(this.y, o.y);
        if (retVal == 0)
            retVal = Double.compare(this.x, o.x);
        return retVal;
    }

    /**
     * @return the x
     */
    public double getX() {
        return this.x;
    }

    /**
     * Specify a new x-value.
     *
     * @param x 	the x to set
     */
    public void setX(double x) {
        this.x = x;
    }

    /**
     * @return the y
     */
    public double getY() {
        return this.y;
    }

    /**
     * Specify a new y-value.
     *
     * @param y 	the y to set
     */
    public void setY(double y) {
        this.y = y;
    }

    @Override
    public String toString() {
        return "(" + Double.toString(this.x) + "," + Double.toString(y) + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(this.x);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(this.y);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Coordinate)) {
            return false;
        }
        Coordinate other = (Coordinate) obj;
        if (Double.doubleToLongBits(this.x) != Double.doubleToLongBits(other.x)) {
            return false;
        }
        if (Double.doubleToLongBits(this.y) != Double.doubleToLongBits(other.y)) {
            return false;
        }
        return true;
    }


}
