package com.polyweather;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * Component 5: Anomaly Detection and Alerting
 *
 * Two input streams:
 *   - market-accuracy-aggregates  → accuracy anomaly detection per city
 *   - market-weather-correlations → arbitrage detection per market
 *
 * Emits alerts to arbitrage-alerts.
 *
 * Alert types:
 *   ACCURACY_DROP    — accuracy fell >20% vs previous reading for a city
 *   ACCURACY_ANOMALY — accuracy deviated >2σ from running baseline
 *   ARBITRAGE        — price sum != 1.0 flagged by upstream producer
 *   HIGH_ERROR       — PREDICTION_ERROR > 0.7 on a single market
 */
public class AnomalyJob {

    static final ObjectMapper MAPPER = new ObjectMapper();
    static final String BOOTSTRAP = "kafka:29092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);

        KafkaSource<String> accuracySource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("market-accuracy-aggregates")
                .setGroupId("anomaly-job-accuracy-v1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        KafkaSource<String> correlationSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("market-weather-correlations")
                .setGroupId("anomaly-job-corr-v1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("arbitrage-alerts")
                        .setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                        .build())
                .build();

        DataStream<String> accuracyStream = env.fromSource(
                accuracySource, WatermarkStrategy.noWatermarks(), "accuracy-aggregates");

        DataStream<String> correlationStream = env.fromSource(
                correlationSource, WatermarkStrategy.noWatermarks(), "correlations");

        // Tag each message with its source before union
        DataStream<String> taggedAccuracy = accuracyStream
                .map(msg -> "{\"_src\":\"accuracy\"," + msg.substring(1));

        DataStream<String> taggedCorrelation = correlationStream
                .map(msg -> "{\"_src\":\"correlation\"," + msg.substring(1));

        taggedAccuracy.union(taggedCorrelation)
                .keyBy(msg -> extractField(msg, "LOCATION_NAME"))
                .flatMap(new AnomalyDetector())
                .filter(s -> s != null)
                .sinkTo(sink);

        env.execute("Polymarket Anomaly Detection");
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

    public static class AnomalyDetector extends RichFlatMapFunction<String, String> {

        // Accuracy anomaly state (keyed by LOCATION_NAME)
        private ValueState<Double> prevAccuracyRate;   // last seen accuracy_rate
        private ValueState<Long>   baselineCount;      // Welford n
        private ValueState<Double> baselineMean;       // Welford mean
        private ValueState<Double> baselineM2;         // Welford M2 (for variance)

        @Override
        public void open(OpenContext ctx) {
            prevAccuracyRate = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("prev-accuracy", Types.DOUBLE));
            baselineCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("baseline-n", Types.LONG));
            baselineMean = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("baseline-mean", Types.DOUBLE));
            baselineM2 = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("baseline-m2", Types.DOUBLE));
        }

        @Override
        public void flatMap(String msg, Collector<String> out) throws Exception {
            JsonNode node;
            try {
                node = MAPPER.readTree(msg);
            } catch (Exception e) {
                return;
            }

            String src = node.path("_src").asText("");
            String location = node.path("LOCATION_NAME").asText("");
            if (location.isEmpty() || "UNKNOWN".equals(location)) return;

            if ("accuracy".equals(src)) {
                handleAccuracy(node, location, out);
            } else if ("correlation".equals(src)) {
                handleCorrelation(node, location, out);
            }
        }

        private void handleAccuracy(JsonNode node, String location, Collector<String> out) throws Exception {
            double accuracyRate = node.path("accuracy_rate").asDouble(Double.NaN);
            long totalCount = node.path("total_count").asLong(0);
            if (Double.isNaN(accuracyRate) || totalCount < 3) return;

            // Update Welford online mean/variance
            long n = orZero(baselineCount.value()) + 1;
            double mean = orZeroD(baselineMean.value());
            double m2 = orZeroD(baselineM2.value());

            double delta = accuracyRate - mean;
            mean += delta / n;
            double delta2 = accuracyRate - mean;
            m2 += delta * delta2;

            baselineCount.update(n);
            baselineMean.update(mean);
            baselineM2.update(m2);

            double stdDev = n >= 2 ? Math.sqrt(m2 / (n - 1)) : 0.0;

            // Check: accuracy drop >20% vs previous reading
            Double prev = prevAccuracyRate.value();
            if (prev != null && (prev - accuracyRate) > 0.20) {
                out.collect(buildAlert("ACCURACY_DROP", location, null,
                        String.format("accuracy dropped %.1f%% → %.1f%% (delta=%.1f%%)",
                                prev * 100, accuracyRate * 100, (prev - accuracyRate) * 100),
                        node));
            }

            // Check: >2σ deviation from baseline (need at least 10 points)
            if (n >= 10 && stdDev > 0.01 && Math.abs(accuracyRate - mean) > 2.0 * stdDev) {
                out.collect(buildAlert("ACCURACY_ANOMALY", location, null,
                        String.format("accuracy=%.1f%% deviates >2σ from baseline mean=%.1f%% σ=%.3f",
                                accuracyRate * 100, mean * 100, stdDev),
                        node));
            }

            prevAccuracyRate.update(accuracyRate);
        }

        private void handleCorrelation(JsonNode node, String location, Collector<String> out) throws Exception {
            String conditionId = node.path("condition_id").asText("");
            boolean arbitrageFlag = node.path("arbitrage_flag").asBoolean(false);
            double predError = node.path("PREDICTION_ERROR").asDouble(Double.NaN);
            double yesPrice = node.path("yes_price").asDouble(Double.NaN);
            double targetTemp = node.path("target_temp").asDouble(Double.NaN);

            if (arbitrageFlag) {
                out.collect(buildAlert("ARBITRAGE", location, conditionId,
                        String.format("price sum != 1.0 | question: %s",
                                node.path("question").asText("")),
                        node));
            }

            if (!Double.isNaN(predError) && predError > 0.70) {
                out.collect(buildAlert("HIGH_ERROR", location, conditionId,
                        String.format("prediction_error=%.3f yes_price=%.3f target=%.1f°C actual=%.1f°C",
                                predError, yesPrice, targetTemp, node.path("actual_temp").asDouble(0)),
                        node));
            }
        }

        private String buildAlert(String alertType, String location, String conditionId,
                                  String detail, JsonNode source) throws Exception {
            ObjectNode alert = MAPPER.createObjectNode();
            alert.put("alert_type", alertType);
            alert.put("LOCATION_NAME", location);
            if (conditionId != null) alert.put("condition_id", conditionId);
            alert.put("detail", detail);
            alert.put("POLL_TIMESTAMP", source.path("POLL_TIMESTAMP").asText(Instant.now().toString()));
            alert.put("ALERT_TIMESTAMP", Instant.now().toString());
            return MAPPER.writeValueAsString(alert);
        }

        private long orZero(Long v) { return v == null ? 0L : v; }
        private double orZeroD(Double v) { return v == null ? 0.0 : v; }
    }
}
