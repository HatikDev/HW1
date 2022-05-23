package bdtc.lab1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Редьюсер: вычисляет среднее значение из данных, полученных от {@link HW1Mapper}
 */
public class HW1Reducer extends Reducer<LongWritable, LongWritable, Text, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        long size = 0;
        long id = key.get() % 60000; // Получение id из ключа
        long time = key.get() - id; // Получение временной метки из ключа
        while (values.iterator().hasNext()) { // Расчёт суммы всех значений
            sum += values.iterator().next().get();
            ++size;
        }
        sum /= size; // Расчёт среднего значения

        context.write(new Text(id + ""), new Text(time + ",1m," + sum));
    }
}
