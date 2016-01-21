package com.midea.cloudSearch.druid.spatial;
public class DistanceFilterParams {
    private String distance;
    private Point from;

    public DistanceFilterParams(String distance, Point from) {
        this.distance = distance;
        this.from = from;
    }

    public String getDistance() {
        return distance;
    }

    public Point getFrom() {
        return from;
    }
}
