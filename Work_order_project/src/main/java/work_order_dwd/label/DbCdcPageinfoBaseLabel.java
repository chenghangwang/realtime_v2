package work_order_dwd.label;

import com.alibaba.fastjson.JSONObject;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.Duration;
import java.util.Date;
import java.util.List;

/**
 * @Package work_order_dwd.label.DbCdcPageinfoBaseLabel
 * @Author chenghang.wang
 * @Date 2025/5/14 16:30
 * @description: 页面
 */
public class DbCdcPageinfoBaseLabel {



    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    "jdbc:mysql://cdh03:3306",
                    "root",
                    "root");
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from realtime.base_category3 as b3  \n" +
                    "     join realtime.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join realtime.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从kafka中读取数据设置时间戳
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        "cdh01:9092",
                        "realtime_log_v1",
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    JSONObject jsonObject = JSONObject.parseObject(event);
                                    if (event != null && jsonObject.containsKey("ts_ms")){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");

//        kafkaCdcDbSource.print();
        SingleOutputStreamOperator<JSONObject> mapkafkaCdcDbSource = kafkaCdcDbSource.map(JSONObject::parseObject);
//        mapkafkaCdcDbSource.print();
        SingleOutputStreamOperator<JSONObject> dataPageLogConvertJsonDs = mapkafkaCdcDbSource.process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("common")) {
                            JSONObject common = jsonObject.getJSONObject("common");
                            result.put("uid", common.getString("uid")!=null ? common.getString("uid") : "home");
                            result.put("ts", common.getString("ts"));
                            JSONObject deviceInfo = new JSONObject();
                            common.remove("sid");
                            common.remove("mid");
                            common.remove("is_new");
                            deviceInfo.putAll(common);
                            result.put("deviceInfo", deviceInfo);
                            if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()) {
                                JSONObject pageInfo = jsonObject.getJSONObject("page");
                                if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")) {
                                    String item = pageInfo.getString("item");
                                    result.put("search_item", item);
                                }
                            }
                        }
                        JSONObject deviceInfo = result.getJSONObject("deviceInfo");
                        String os = deviceInfo.getString("os").split(" ")[0];
                        deviceInfo.put("os", os);
                        collector.collect(result);
                    }
                })
                .uid("get device info & search")
                .name("get device info & search");
        dataPageLogConvertJsonDs.print();



        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = dataPageLogConvertJsonDs.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));
//      keyedStreamLogPageMsg.print();
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsDataFunc());
//      processStagePageLogDs.print();

        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");


//        win2MinutesPageLogsDs.print();

        win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories,device_rate_weight_coefficient,search_rate_weight_coefficient))
                .print();

        env.execute();
    }
}
