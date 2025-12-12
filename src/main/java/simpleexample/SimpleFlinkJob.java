package simpleexample;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;   


public class SimpleFlinkJob {

    public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a simple input data stream
        DataStream<String> inputStream = env.fromElements("one", "two", "three", "four", "five",
                "six", "seven", "eight", "nine", "ten");

        // Perform a simple transformation
        DataStream<Tuple2<String, Integer>> resultStream = inputStream
                .map(value -> new Tuple2<>(value, value.length()))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // Add the custom sink
        resultStream.addSink(new CustomSink());

        // Execute the Flink job
        env.execute("Simple Flink Job");
    }

}
