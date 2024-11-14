package com.example;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    private DoubleWritable sortKey = new DoubleWritable();
    private Text weekdays = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split("\\t");
        if (columns.length != 2) {
            return;
        }

        String weekday = columns[0];
        String[] amounts = columns[1].split(",");
        if (amounts.length != 2) {
            return;
        }

        try {
            double averageIn = Double.parseDouble(amounts[0]);
            sortKey.set(averageIn);
            weekdays.set(weekday + "\t" + amounts[0] + "," + amounts[1]);
            context.write(sortKey, weekdays);
        } catch (NumberFormatException e) {
            // Log the exception for debugging purposes
            System.err.println("NumberFormatException: " + e.getMessage());
        }
    }
}
