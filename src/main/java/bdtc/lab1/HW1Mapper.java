package bdtc.lab1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HW1Mapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Разбивает строку на массив строк по запятым и получает id записи, временную метку и значение

        try {
            String[] parts = value.toString().split(",");
            long recordId = Long.parseLong(parts[0]);
            long recordTimestamp = Long.parseLong(parts[1]);
            long recordValue = Long.parseLong(parts[2]);

            // Вычисляет пару ключ-значение для {@link HW1Reducer}
            LongWritable finalKey = new LongWritable(recordTimestamp - recordTimestamp % 60000 + recordId);
            LongWritable finalValue = new LongWritable(recordValue);

            context.write(finalKey, finalValue);
        } catch(Exception e) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}
