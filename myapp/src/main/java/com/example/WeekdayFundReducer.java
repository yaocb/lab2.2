package com.example;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeekdayFundReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(WeekdayFundReducer.class);
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        double all_in = 0.0;
        double all_out = 0.0;
        Set<String> specificDates = new HashSet<>();
        Map<String, Double[]> amountList = new HashMap<>();

        for (Text val : values) {
            String[] columns = val.toString().split(",");
            if (columns.length < 3) {
                continue;
            }

            try {
                String date = columns[0];
                double input = Double.parseDouble(columns[1]);
                double output = Double.parseDouble(columns[2]);

                specificDates.add(date);

                if (amountList.containsKey(date)) {
                    Double[] amounts = amountList.get(date);
                    amounts[0] += input;
                    amounts[1] += output;
                } else {
                    amountList.put(date, new Double[]{input, output});
                }
            } catch (NumberFormatException e) {
                logger.error("NumberFormatException: " + e.getMessage(), e);
                continue;
            }
        }

        int dateCount = specificDates.size();
        if (dateCount == 0) {
            return;
        }
        
        for (Double[] amounts : amountList.values()) {
            all_in += amounts[0];
            all_out += amounts[1];
        }

        double averageIn = all_in / dateCount;
        double averageOut = all_out / dateCount;
        result.set(averageIn + "," + averageOut);
        context.write(key, result);
    }
}
