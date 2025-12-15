package simpleexample;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;   


public class SimpleFlinkJob {

    public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a simple input data stream
        DataStream<String> inputStream = env
                .fromElements("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten")
                .name("input-data");

        // Perform a simple transformation
        DataStream<Tuple2<String, Integer>> resultStream = inputStream
                .map(value -> new Tuple2<>(value, value.length()))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .name("map-to-length");

        // Add the custom sink
        resultStream
                .addSink(new CustomSink())
                .name("custom-sink");

        // Execute the Flink job
        env.execute("Simple Flink Job");
    }

}
