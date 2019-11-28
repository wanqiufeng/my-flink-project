package myflink;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.function.Predicate;

/**
 * @author created by chenjun at 2019-11-28 10:47
 */
public class LowLevelAPITest {
    public void main(String args[]) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, String>> text = null;
        KeyedStream<Tuple2<String, String>, String> tuple2StringKeyedStream = text.keyBy(new KeySelector<Tuple2<String, String>, String>() {

            @Override
            public String getKey(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2.f0;
            }
        });
    }


    public class StartEndDuration
            extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Long>> {

        private ValueState<Long> startTime;

        @Override
        public void open(Configuration conf) {
            // obtain state handle
            startTime = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Long>("startTime", Long.class));
        }

        /** Called for each processed event. */
        @Override
        public void processElement(
                Tuple2<String, String> in,
                Context ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            switch (in.f1) {
                case "START":
                    // set the start time if we receive a start event.
                    startTime.update(ctx.timestamp());
                    // register a timer in four hours from the start event.
                    ctx.timerService()
                            .registerEventTimeTimer(ctx.timestamp() + 4 * 60 * 60 * 1000);
                    break;
                case "END":
                    // emit the duration between start and end event
                    Long sTime = startTime.value();
                    if (sTime != null) {
                        out.collect(Tuple2.of(in.f0, ctx.timestamp() - sTime));
                        // clear the state
                        startTime.clear();
                    }
                default:
                    // do nothing
            }
        }

        /** Called when a timer fires. */
        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple2<String, Long>> out) {

            // Timeout interval exceeded. Cleaning up the state.
            startTime.clear();
        }
    }
}
