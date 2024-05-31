package com.atguigu.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class MyAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //  姓名，分数，权重
        DataStreamSource<Tuple3<Integer,String, String>> scoreWeightDS = env.fromElements(
                Tuple3.of(1001,"runxi", "900"),
                Tuple3.of(1001,"牛仔裤", "353"),
                Tuple3.of(1001,"紫色防晒服", "893"),
                Tuple3.of(1002,"小包包", "600"),
                Tuple3.of(1002,"旗袍 合身", "590"),
                Tuple3.of(1002,"裤", "192")
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table scoreWeightTable = tableEnv.fromDataStream(scoreWeightDS, $("f0").as("user_id"),$("f1").as("keyword"), $("f2").as("search_time"));
        tableEnv.createTemporaryView("scores", scoreWeightTable);

        // TODO 2.注册函数
        tableEnv.createTemporaryFunction("collect_keyword", CollectKeyword.class);

        // 设置最大列宽度
        System.setProperty("table.print.max-field-width", "10000");

        // TODO 3.调用 自定义函数
        TableResult result = tableEnv
                .sqlQuery("select user_id,collect_keyword(keyword,search_time) keyword_list from scores group by user_id")
                .execute();

        result.print();
        System.out.println(result);


    }


    // TODO 1.继承 AggregateFunction< 返回类型，累加器类型<加权总和，权重总和> >
    public static class CollectKeyword extends AggregateFunction<String, List<Map<String, String>>> {

        @Override
        public String getValue(List<Map<String, String>> ls) {
            // 根据 map 中的 search_time 字段排序
            Collections.sort(ls, new Comparator<Map<String, String>>() {
                @Override
                public int compare(Map<String, String> o1, Map<String, String> o2) {
                    String time1 = o1.get("search_time");
                    String time2 = o2.get("search_time");
                    return time2.compareTo(time1);
                }
            });
            return ls.toString().replace("keyword=", "").replace("search_time=", "").replaceAll(",\\s(?!\\{)", ":");
        }

        @Override
        public List<Map<String, String>> createAccumulator() {return new ArrayList<>();}

        /**
         * 累加计算的方法，每来一行数据都会调用一次
         * @param acc 累加器类型
         * @param keyword 第一个参数：关键词
         * @param search_time 第二个参数：搜索时间
         */
        public void accumulate(List<Map<String, String>> acc,String keyword,String search_time){
            Map<String, String> keywordMap = new HashMap<>();
            keywordMap.put("keyword", keyword);
            keywordMap.put("search_time", search_time);
            System.out.println(keywordMap);
            acc.add(keywordMap);
        }
    }

}
