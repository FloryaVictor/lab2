package lab2.mappers;

import lab2.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("\"YEAR\",\"QUARTER\",\"MONTH\",\"DAY_OF_MONTH\",\"DAY_OF_WEEK\",\"FL_DATE\",\"UNIQUE_CARRIER\",\"AIRLINE_ID\",\"CARRIER\",\"TAIL_NUM\",\"FL_NUM\",\"ORIGIN_AIRPORT_ID\",\"ORIGIN_AIRPORT_SEQ_ID\",\"ORIGIN_CITY_MARKET_ID\",\"DEST_AIRPORT_ID\",\"WHEELS_ON\",\"ARR_TIME\",\"ARR_DELAY\",\"ARR_DELAY_NEW\",\"CANCELLED\",\"CANCELLATION_CODE\",\"AIR_TIME\",\"DISTANCE\",")){
            return;
        }
        String[] info = value.toString().replaceAll("\"","").split(",");
        try {
            if (Float.parseFloat(info[18]) > 0) {
                context.write(new TextPair(info[14], "1"), new Text(info[18]));
                System.out.println(info[18]);
            }
        }catch (Exception e) {
            if (e.getClass() == IOException.class || e.getClass() == InterruptedException.class){
                throw e;
            }
        }
    }
}
