package com.polyweather;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;

/**
 * Component 4: Accuracy Aggregation
 *
 * Reads market-weather-correlations and maintains rolling accuracy stats per city.
 * Emits an updated aggregate record after every incoming correlation.
 *
 * Separate stats for:
 *   - forecast-based (open markets, intra-day signal)
 *   - historical-based (closed markets, ground truth)
 */
public class AccuracyJob {

    static final ObjectMapper MAPPER = new ObjectMapper();
    static final String BOOTSTRAP = "kafka:29092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);

        KafkaSource<String> correlationSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("market-weather-correlations")
                .setGroupId("accuracy-job-v1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("market-accuracy-aggregates")
                        .setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                        .build())
                .build();

        DataStream<String> correlations = env.fromSource(
                correlationSource, WatermarkStrategy.noWatermarks(), "correlations");

        correlations
                .keyBy(msg -> extractField(msg, "LOCATION_NAME"))
                .map(new AccuracyAggregator())
                .filter(s -> s != null)
                .sinkTo(sink);

        env.execute("Polymarket Accuracy Aggregation");
    }

    static String extractField(String json, String field) {
        try {
            JsonNode node = MAPPER.readTree(json);
            JsonNode val = node.get(field);
            return val != null && !val.isNull() ? val.asText() : "UNKNOWN";
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    public static class AccuracyAggregator extends RichMapFunction<String, String> {

        // All correlations (forecast + historical)
        private ValueState<Long> totalCount;
        private ValueState<Long> correctCount;
        private ValueState<Double> sumPredictionError;

        // Closed market correlations only (ground truth)
        private ValueState<Long> closedCount;
        private ValueState<Long> closedCorrectCount;
        private ValueState<Double> closedSumError;

        // Largest observed prediction errors (arbitrage signals)
        private ValueState<Double> maxPredictionError;

        @Override
        public void open(OpenContext ctx) {
            totalCount          = getRuntimeContext().getState(new ValueStateDescriptor<>("total-count", Types.LONG));
            correctCount        = getRuntimeContext().getState(new ValueStateDescriptor<>("correct-count", Types.LONG));
            sumPredictionError  = getRuntimeContext().getState(new ValueStateDescriptor<>("sum-error", Types.DOUBLE));
            closedCount         = getRuntimeContext().getState(new ValueStateDescriptor<>("closed-count", Types.LONG));
            closedCorrectCount  = getRuntimeContext().getState(new ValueStateDescriptor<>("closed-correct-count", Types.LONG));
            closedSumError      = getRuntimeContext().getState(new ValueStateDescriptor<>("closed-sum-error", Types.DOUBLE));
            maxPredictionError  = getRuntimeContext().getState(new ValueStateDescriptor<>("max-error", Types.DOUBLE));
        }

        @Override
        public String map(String correlationJson) throws Exception {
            JsonNode c;
            try {
                c = MAPPER.readTree(correlationJson);
            } catch (Exception e) {
                return null;
            }

            String location = c.path("LOCATION_NAME").asText("");
            if (location.isEmpty() || "UNKNOWN".equals(location)) return null;

            int outcome = c.path("ACTUAL_OUTCOME").asInt(-1);
            double error = c.path("PREDICTION_ERROR").asDouble(Double.NaN);
            boolean isClosed = c.path("market_closed").asBoolean(false);

            if (outcome < 0 || Double.isNaN(error)) return null;

            // Update all-time stats
            long tc = orZero(totalCount.value()) + 1;
            long cc = orZero(correctCount.value()) + (outcome == 1 ? 1 : 0);
            double se = orZeroD(sumPredictionError.value()) + error;
            double maxErr = Math.max(orZeroD(maxPredictionError.value()), error);

            totalCount.update(tc);
            correctCount.update(cc);
            sumPredictionError.update(se);
            maxPredictionError.update(maxErr);

            // Update closed-market (ground truth) stats
            long clc = orZero(closedCount.value());
            long clcc = orZero(closedCorrectCount.value());
            double clse = orZeroD(closedSumError.value());
            if (isClosed) {
                clc++;
                clcc += (outcome == 1 ? 1 : 0);
                clse += error;
                closedCount.update(clc);
                closedCorrectCount.update(clcc);
                closedSumError.update(clse);
            }

            double accuracyRate = tc > 0 ? (double) cc / tc : 0.0;
            double avgError = tc > 0 ? se / tc : 0.0;
            double closedAccuracy = clc > 0 ? (double) clcc / clc : 0.0;
            double closedAvgError = clc > 0 ? clse / clc : 0.0;

            ObjectNode result = MAPPER.createObjectNode();
            result.put("LOCATION_NAME", location);
            result.put("total_count", tc);
            result.put("correct_count", cc);
            result.put("accuracy_rate", round(accuracyRate));
            result.put("avg_prediction_error", round(avgError));
            result.put("max_prediction_error", round(maxErr));
            result.put("closed_count", clc);
            result.put("closed_correct_count", clcc);
            result.put("closed_accuracy_rate", round(closedAccuracy));
            result.put("closed_avg_error", round(closedAvgError));
            result.put("last_question_type", c.path("question_type").asText(""));
            result.put("LAST_UPDATED", Instant.now().toString());

            return MAPPER.writeValueAsString(result);
        }

        private long orZero(Long v) { return v == null ? 0L : v; }
        private double orZeroD(Double v) { return v == null ? 0.0 : v; }

        private double round(double v) {
            return Math.round(v * 10000.0) / 10000.0;
        }
    }
}
