package realtime_dws;

import Base.BaseApp;
import Constat.constat;
import bean.TablepenviceOrderBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import utils.Hbaseutli;
import utils.dataformtutil;
import utils.finksink;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
/**
 * @Package realtime_dws.dws_trade_province_order_window
 * @Author ayang
 * @Date 2025/4/16 15:40
 * @description: 省份交易异部   doris导入
 */

public class dws_trade_province_order_window extends BaseApp {
    public static void main(String[] args) throws Exception {
        new dws_trade_province_order_window().
                start(10030,4,
                        "dws_trade_province_order_window",
                        constat.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.过滤空消息  并对流中数据进行类型转换    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

//        jsonObjDS.print();
//        2> {"create_time":"1744063561000","sku_num":"2","split_original_amount":"2598.0000","split_coupon_amount":"0.00","sku_id":"6","user_id":"472","province_id":"26","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米","id":"2259","order_id":"1490","split_activity_amount":"0.00","ts_ms":1744554504399,"split_total_amount":"2598.00"}

//        //TODO 2.按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
//        orderDetailIdKeyedDS.print();
        //        2> {"create_time":"1744063561000","sku_num":"2","split_original_amount":"2598.0000","split_coupon_amount":"0.00","sku_id":"6","user_id":"472","province_id":"26","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米","id":"2259","order_id":"1490","split_activity_amount":"0.00","ts_ms":1744554504399,"split_total_amount":"2598.00"}

//        //TODO 3.去重
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //重复  需要对影响到度量值的字段进行取反 发送到下游
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
//        distinctDS.print();
//        3> {"create_time":"1744401605000","sku_num":"1","split_original_amount":"999.0000","split_coupon_amount":"0.00","sku_id":"4","user_id":"454","province_id":"26","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 冰雾白 游戏智能手机 小米 红米","id":"2259","order_id":"1421","split_activity_amount":"0.00","ts_ms":1744554510334,"split_total_amount":"-999.00"}
//        3> {"create_time":"1744063561000","sku_num":"2","split_original_amount":"2598.0000","split_coupon_amount":"0.00","sku_id":"6","user_id":"472","province_id":"26","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米","id":"2259","order_id":"1490","split_activity_amount":"0.00","ts_ms":1744554504399,"split_total_amount":"2598.00"}
//        3> {"create_time":"1744063561000","sku_num":"2","split_original_amount":"2598.0000","split_coupon_amount":"0.00","sku_id":"6","user_id":"472","province_id":"26","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米","id":"2259","order_id":"1490","split_activity_amount":"0.00","ts_ms":1744554504399,"split_total_amount":"-2598.00"}
//        3> {"create_time":"1744063561000","sku_num":"2","split_original_amount":"2598.0000","split_coupon_amount":"0.00","sku_id":"6","user_id":"472","province_id":"26","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米","id":"2259","order_id":"1490","split_activity_amount":"0.00","ts_ms":1744595916797,"split_total_amount":"2598.00"}
//        3> {"create_time":"1744063561000","sku_num":"2","split_original_amount":"2598.0000","split_coupon_amount":"0.00","sku_id":"6","user_id":"472","province_id":"26","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米","id":"2259","order_id":"1490","split_activity_amount":"0.00","ts_ms":1744595916797,"split_total_amount":"-2598.00"}
//        3> {"create_time":"1744063561000","sku_num":"2","split_original_amount":"2598.0000","split_coupon_amount":"0.00","sku_id":"6","user_id":"472","province_id":"26","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米","id":"2259","order_id":"1490","split_activity_amount":"0.00","ts_ms":1744419903839,"split_total_amount":"2598.00"}


//        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts_ms") ;
                                    }
                                }
                        )
        );
//        withWatermarkDS.print();
//        1> {"create_time":"1744406535000","sku_num":"1","split_original_amount":"129.0000","split_coupon_amount":"30.00","sku_id":"26","coupon_id":"1","user_id":"31","province_id":"7","sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 ","id":"2310","order_id":"1452","split_activity_amount":"0.00","ts_ms":1744554510352,"split_total_amount":"99.00"}

//        //TODO 5.再次对流中数据进行类型转换  jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TablepenviceOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TablepenviceOrderBean>() {
                    @Override
                    public TablepenviceOrderBean map(JSONObject jsonObj) throws Exception {
                        //{"create_time":"2024-06-11 19:35:25","sku_num":"1","activity_rule_id":"1","split_original_amount":"6999.0000","split_coupon_amount":"0.0",
                        // "sku_id":"2","date_id":"2024-06-11","user_id":"616","province_id":"17","activity_id":"1","sku_name":"小米","id":"19772","order_id":"13959",
                        // "split_activity_amount":"500.0","split_total_amount":"6499.0","ts":1718278525}
                        String provinceId = jsonObj.getString("province_id");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts");
                        String orderId = jsonObj.getString("order_id");

                        TablepenviceOrderBean orderBean = TablepenviceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderAmount(splitTotalAmount)
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                .ts(ts)
                                .build();
                        return orderBean;
                    }
                }
        );
//        beanDS.print();
//        //TODO 6.分组
        KeyedStream<TablepenviceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TablepenviceOrderBean::getProvinceId);
//        provinceIdKeyedDS.print();
//        2> TablepenviceOrderBean(stt=null, edt=null, curDate=null, provinceId=1, provinceName=, orderCount=null, orderAmount=-6029.10, ts=null, orderIdSet=[1716])

//        //TODO 7.开窗
        WindowedStream<TablepenviceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
//
//        //TODO 8.聚合
        SingleOutputStreamOperator<TablepenviceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TablepenviceOrderBean>() {
                    @Override
                    public TablepenviceOrderBean reduce(TablepenviceOrderBean value1, TablepenviceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TablepenviceOrderBean, TablepenviceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TablepenviceOrderBean> input, Collector<TablepenviceOrderBean> out) throws Exception {
                        TablepenviceOrderBean orderBean = input.iterator().next();
                        String stt = dataformtutil.tsToDateTime(window.getStart());
                        String edt = dataformtutil.tsToDateTime(window.getEnd());
                        String curDate = dataformtutil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
        );

//        reduceDS.print();
//        1> TablepenviceOrderBean(stt=2025-04-13 22:28:30, edt=2025-04-13 22:28:40, curDate=2025-04-13, provinceId=33, provinceName=, orderCount=76, orderAmount=153193.50, ts=null, orderIdSet=[89, 1583, 151, 1932, 154, 1216, 1930, 156, 1851, 996, 118, 955, 51, 1735, 54, 55, 57, 1908, 18, 1470, 1074, 162, 1745, 1149, 1666, 168, 763, 1587, 169, 1223, 1586, 1222, 966, 1906, 1707, 1946, 1901, 1080, 66, 23, 29, 130, 1679, 1831, 134, 1874, 179, 1278, 1870, 139, 72, 74, 1516, 34, 38, 39, 1693, 1646, 1921, 143, 1128, 1721, 1446, 1523, 1841, 102, 1245, 2015, 1200, 989, 83, 1968, 949, 1802, 1406, 42])

//        //TODO 9.关联省份维度
        SingleOutputStreamOperator<TablepenviceOrderBean> map = reduceDS.map(new RichMapFunction<TablepenviceOrderBean, TablepenviceOrderBean>() {
            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = Hbaseutli.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                Hbaseutli.closeHBaseConnection(hbaseConn);
            }

            @Override
            public TablepenviceOrderBean map(TablepenviceOrderBean tablepenviceOrderBean) throws Exception {
                String provinceId = tablepenviceOrderBean.getProvinceId();
                JSONObject row = Hbaseutli.getRow(hbaseConn, "realtime_v1", "dim_base_province", provinceId, JSONObject.class);
                tablepenviceOrderBean.setProvinceName(row.getString("name"));
                return tablepenviceOrderBean;
            }
        });
        SingleOutputStreamOperator<String> map1 = map.map(new RichMapFunction<TablepenviceOrderBean, String>() {
            @Override
            public String map(TablepenviceOrderBean tablepenviceOrderBean) throws Exception {
                return JSON.toJSONString(tablepenviceOrderBean);
            }
        });
        map1.print();

//        2> {"curDate":"2025-04-13","edt":"2025-04-13 22:28:40","orderAmount":157122.69,"orderCount":87,"provinceId":"28","provinceName":"海南","stt":"2025-04-13 22:28:30"}

        //        //TODO 10.将关联的结果写到Doris中

        map1.sinkTo(finksink.getDorisSink("dws_trade_province_order_window"));

    }
}
