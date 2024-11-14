package com.example;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    private Text dateText = new Text();
    private Text flowText = new Text();

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            String[] columns = val.toString().split(",");
            if (columns.length != 3) {
                continue;
            }
            dateText.set(columns[0]);
            flowText.set(columns[1] + "," + columns[2]);
            context.write(dateText, flowText);
        }
    }
}
