import eu.bitwalker.useragentutils.UserAgent;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, LongWritable, LongWritable> mapDriver;
    private ReduceDriver<LongWritable, LongWritable, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, LongWritable, LongWritable, Text, Text> mapReduceDriver;

    private final String testInput1 = "1,1652649720003,100";
    private final String testInput2 = "1,1652649720050,114";
    private final String correctOutput = "1652649720000,1m,107";

    private UserAgent userAgent;
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testInput2))
                .withOutput(new LongWritable(1652649720001L), new LongWritable(114))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(100));
        values.add(new LongWritable(114));
        reduceDriver
                .withInput(new LongWritable(1652649720001L), values)
                .withOutput(new Text(1 + ""), new Text(correctOutput))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testInput1))
                .withInput(new LongWritable(), new Text(testInput2))
                .withOutput(new Text(1 + ""), new Text(correctOutput))
                .runTest();
    }
}
