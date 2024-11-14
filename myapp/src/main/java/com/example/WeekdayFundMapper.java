package com.example;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Date; 

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParser;

public class WeekdayFundMapper extends Mapper<Object, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(WeekdayFundMapper.class);
    private Text weekdayKeys = new Text();
    private Text Flow = new Text();
    private boolean firstLine = true;
    private CSVParser csvParser;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.isEmpty()) {
            return;
        }
        if (firstLine && line.contains("report_date")) {
            firstLine = false;
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < 9) {
            return;
        }
    
        try {
            String input = fields[4].trim();
            String output = fields[8].trim();
            if (input.isEmpty()) {
                input = "0";
            }
            if (output.isEmpty()) {
                output = "0";
            }

            Date reportedDate = dateFormat.parse(fields[1].trim());
            SimpleDateFormat dayFormat = new SimpleDateFormat("EEEE", Locale.ENGLISH);
            String weekday = dayFormat.format(reportedDate);
            String dateString = dateFormat.format(reportedDate);
            
            weekdayKeys.set(weekday);
            Flow.set(dateString + "," + input + "," + output);
            context.write(weekdayKeys, Flow);

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }
}
