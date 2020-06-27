package com.ioteye.gta.flink.examples.mcep;

import com.ioteye.gta.flink.mcep.*;
import com.ioteye.gta.flink.mcep.pattern.Pattern;
import com.ioteye.gta.flink.mcep.pattern.conditions.IterativeCondition;
import com.ioteye.gta.flink.mcep.pattern.conditions.TimeIterativeCondition;
import com.ioteye.gta.flink.mcep.rule.RuleEvent;
import com.ioteye.gta.flink.mcep.rule.RulePattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.*;

@Slf4j
public class GtaMCepExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> input = env.socketTextStream("localhost", 9999);


        SingleOutputStreamOperator<MetrixEvent> inputStream = input.map((ele) -> {
            String[] words = ele.split(",");

            String userName = words[0];
            String ip = words[1];
            String type = words[2];
            long timestamp = Long.valueOf(words[3]);

            MetrixEvent event = new MetrixEvent();
            event.setKey("login");
            event.setEventId(1);
            event.setUserName(userName);
            event.setIp(ip);
            event.setType(type);
            event.setTimestamp(timestamp);

            System.out.println("event = " + event);

            return event;
        });

        InjectionPatternFunction<MetrixEvent> injectionPatternFunction = new InjectionPatternFunction<MetrixEvent>() {
            @Override
            public void open() throws Exception {
                // not action
            }

            @Override
            public List<RulePattern<MetrixEvent>> injectPatterns() throws Exception {
                Pattern<MetrixEvent, MetrixEvent> loginFailPattern = Pattern.<MetrixEvent>
                        begin("first")
                        .where(new TimeIterativeCondition<MetrixEvent>() {
                            @Override
                            public int step(MetrixEvent event, Context<MetrixEvent> ctx) throws Exception {
                                return 1;
                            }

                            @Override
                            public int conditionId(MetrixEvent event, Context<MetrixEvent> ctx) throws Exception {
                                return 1;
                            }

                            @Override
                            public boolean timeFilter(MetrixEvent event, Context ctx) throws Exception {
                                System.out.println("first: " + event);
                                return event.getType().equals("fail");
                            }
                        })
                        .next("second")
                        .where(new TimeIterativeCondition<MetrixEvent>() {
                            @Override
                            public int step(MetrixEvent event, Context<MetrixEvent> ctx) throws Exception {
                                return 1;
                            }

                            @Override
                            public int conditionId(MetrixEvent event, Context<MetrixEvent> ctx) throws Exception {
                                return 2;
                            }

                            @Override
                            public boolean timeFilter(MetrixEvent event, Context ctx) throws Exception {
                                System.out.println("second: " + event);
                                return event.getType().equals("fail");
                            }
                        })
                        .within(Time.seconds(10))
                        .next("three")
                        .where(new TimeIterativeCondition<MetrixEvent>() {
                            @Override
                            public int step(MetrixEvent event, Context<MetrixEvent> ctx) throws Exception {
                                return 2;
                            }
                            @Override
                            public int conditionId(MetrixEvent event, Context<MetrixEvent> ctx) throws Exception {
                                return 3;
                            }

                            @Override
                            public boolean timeFilter(MetrixEvent event, Context ctx) throws Exception {
                                System.out.println("three: " + event);
                                return event.getType().equals("fail");
                            }
                        })
                        .within(Time.seconds(10));

                RuleEvent<MetrixEvent> ruleEvent = new RuleEvent<>();
                ruleEvent.setEventId(1);

                Set<RuleEvent<MetrixEvent>> ruleEventSet = new HashSet<>();
                ruleEventSet.add(ruleEvent);

                RulePattern<MetrixEvent> rulePattern = new RulePattern<>();
                rulePattern.setPattern(loginFailPattern);
                rulePattern.setPatternId(1);
                rulePattern.setRuleEvents(ruleEventSet);

                ArrayList<RulePattern<MetrixEvent>>  injectPatterns = new ArrayList<>();
                injectPatterns.add(rulePattern);

                return injectPatterns;
            }

            @Override
            public long refreshPeriod() {
                return 10;
            }
        };

        DataStream<MetrixEvent> partitionedInput = inputStream.keyBy(new KeySelector<MetrixEvent, Object>() {
            @Override
            public Object getKey(MetrixEvent metrixEvent) throws Exception {
                return metrixEvent.getUserName();
            }
        });

        PatternStream<MetrixEvent> metrixEventPatternStream = GtaCEP.injectPattern(partitionedInput, injectionPatternFunction);

        OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        SingleOutputStreamOperator<String> select = metrixEventPatternStream.select(
                outputTag,
                new PatternTimeoutFunction<MetrixEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<MetrixEvent>> pattern, long timeoutTimestamp) throws Exception {
                        System.out.println("timeout = " + pattern);
                        return pattern.toString();
                    }
                },
                new PatternSelectFunction<MetrixEvent, String>() {
                    @Override
                    public String select(Map<String, List<MetrixEvent>> pattern) throws Exception {
                        System.out.println("select = " + pattern);

                        return pattern.toString();
                    }
                }
        );

        select.print();
        select.getSideOutput(outputTag).print();

        env.execute("mcep");
    }
}
