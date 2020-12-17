package com.neo4j.sync.listener;

import org.neo4j.kernel.availability.AvailabilityListener;

public class InitializationListener implements AvailabilityListener {
    @Override
    public void available() {
        System.out.println("--> AVAILABLE");
    }

    @Override
    public void unavailable() {
        System.out.println("--> NOT AVAILABLE");
    }
}
