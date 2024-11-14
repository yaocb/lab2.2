package com.example;

import org.apache.hadoop.io.DoubleWritable;

public class DescendingDouble extends DoubleWritable.Comparator {
    public int compare(byte[] x1, int y1, int z1, byte[] x2, int y2, int z2) {
        return -super.compare(x1, y1, z1, x2, y2, z2);
    }

    @Override
    public int compare(Object x, Object y) {
        return -super.compare(x, y);
    }
}
