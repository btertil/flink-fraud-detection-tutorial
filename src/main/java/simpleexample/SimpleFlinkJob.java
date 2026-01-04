package simpleexample;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;


public class SimpleFlinkJob {

    public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a simple input data stream
        DataStream<String> inputStream = env
                .fromElements("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "thirty three", "forty four", "fifty three", "fifty five")
                .name("input-data");

        // Perform a simple transformation
        DataStream<Tuple2<String, Integer>> wordLengths = inputStream
                .map(value -> new Tuple2<>(value, value.length()))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .name("map-to-length");

        // Add the custom sink
        wordLengths
                .addSink(new CustomSink("wordLengths-sink"))
                .name("wordLengths-sink");

        DataStream<Tuple2<String, Integer>> wordLengthsFiltered = wordLengths
                .filter(value -> value.f1 > 4)
                .name("filtered");

        wordLengthsFiltered
                .addSink(new CustomSink("wordLengths-filtered-sink"))
                .name("wordLengths-filtered-sink");

        // Occurencies with CustomTokenizer
        DataStream<Tuple2<String, Integer>> occurenceStream = inputStream
                .flatMap(new CustomTokenizer())
                .keyBy(value -> value.f0)
                .sum(1)
                .name("wordcounts-sum");

        occurenceStream
                .addSink(new CustomSink("occurenceStream-sink"))
                .name("occurenceStream-sink");
        
        
                
        // Execute the Flink job
        env.execute("Simple Flink Job");
    }

    public static class CustomTokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.split("\\s+");
            for (String token : tokens) {
                out.collect(new Tuple2<>(token, 1));
            }
        }
    }

}
