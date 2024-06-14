package com.atguigu.z_study;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class scheduleTest {
    // 使用CopyOnWriteArrayList保证线程安全
    private static List<String> dataList = new CopyOnWriteArrayList<>();

    public static void updateList() {
        int size = dataList.size() +1;
        dataList.add("hello" + Integer.toString(size));
        System.out.println("List 数据已更新: " + dataList);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(dataList);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(scheduleTest::updateList, 0, 1, TimeUnit.MINUTES); // 每隔1分钟执行一次更新List操作

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("116.198.36.249", 7777);
        source.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        if (dataList.contains(s)) {
                            return new Tuple2<>(s, 5);
                        } else {
                            return new Tuple2<>(s, 1);
                        }
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                }).sum(1)
                .print();

        env.execute();
    }
}
