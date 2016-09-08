package com.webtrends.harness.component.zookeeper;

public class WookieeServiceDetails {

    // Used by serializer
    public WookieeServiceDetails() {
    }

    public WookieeServiceDetails(int weight) {
        this.weight = weight;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    private int weight;
}
