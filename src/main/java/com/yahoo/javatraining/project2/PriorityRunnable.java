package com.yahoo.javatraining.project2;

/**
 * Created by cdsurfer on 6/22/16.
 */

public abstract class PriorityRunnable implements Comparable<PriorityRunnable>, Runnable {
    private int priority;

    public int getPriority() {
        return priority;
    }

    public PriorityRunnable(int priority) {
        if (priority < 0) {
            throw new IllegalArgumentException();
        }
        this.priority = priority;
    }

    @Override
    public int compareTo(PriorityRunnable other) {
        return getPriority() > other.getPriority() ? -1 : 0;
    }

    @Override
    public void run() { }
}