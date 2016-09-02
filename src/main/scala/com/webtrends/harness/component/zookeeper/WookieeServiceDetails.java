package com.webtrends.harness.component.zookeeper;

public class WookieeServiceDetails {

    // Used by serializer
    public WookieeServiceDetails() {
    }

    public WookieeServiceDetails(int weight) {
        this.weight = weight;
    }

    // Used by serializer
    public int getWeight() {
        return weight;
    }

    // Used by serializer
    public void setWeight(int weight) {
        this.weight = weight;
    }

    private int weight;
}
