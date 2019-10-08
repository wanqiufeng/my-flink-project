package myflink;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by chenjun on 2019-10-01 09:44
 */
public class FoldTest {
    public static void main(String[] args) throws Exception {

        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.
        KeyedStream<Integer, Tuple> integerTupleKeyedStream = env.fromElements(1, 2, 3)
                .keyBy(0);


        integerTupleKeyedStream.fold("start", new FoldFunction<Integer, String>() {
            @Override
            public String fold(String current, Integer value) {
                return current + "-" + value;
            }
        }).print("输出前缀>>>>");
        env.execute("test state");
    }

}
