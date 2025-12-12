package simpleexample;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomSink implements SinkFunction<Tuple2<String, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomSink.class);

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        // Custom sink logic (e.g., write to a database or external system)
        LOG.info("Sink received: " + value);
    }

}
