package com.polyweather;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * Component 4: Accuracy Aggregation
 *
 * Sliding 1-hour window, 15-minute slide (processing time).
 * Emits one aggregate per city per slide — not per message.
 *
 * Metrics per window:
 *   - accuracy_rate           overall % correct
 *   - avg_prediction_error    mean absolute error
 *   - max_prediction_error    worst single error in window
 *   - bias_score              mean signed error (+ = over-predicting Yes)
 *   - closed_accuracy_rate    ground-truth only (closed markets)
 *   - total_count / correct_count
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
                .setGroupId("accuracy-job-v2")
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
                .filter(msg -> {
                    try {
                        JsonNode n = MAPPER.readTree(msg);
                        String loc = n.path("LOCATION_NAME").asText("");
                        return !loc.isEmpty() && !"UNKNOWN".equals(loc);
                    } catch (Exception e) { return false; }
                })
                .keyBy(msg -> {
                    try { return MAPPER.readTree(msg).path("LOCATION_NAME").asText("UNKNOWN"); }
                    catch (Exception e) { return "UNKNOWN"; }
                })
                .window(SlidingProcessingTimeWindows.of(
                        Duration.ofHours(1),
                        Duration.ofMinutes(15)))
                .aggregate(new AccuracyAccumulator(), new WindowResultFunction())
                .filter(s -> s != null)
                .sinkTo(sink);

        env.execute("Polymarket Accuracy Aggregation");
    }

    // --- Accumulator ---

    static class Acc {
        String location = "";
        long total = 0, correct = 0;
        long closedTotal = 0, closedCorrect = 0;
        double sumError = 0, maxError = 0;
        double sumSignedError = 0; // for bias: (yes_price - actual_outcome)
    }

    static class AccuracyAccumulator implements AggregateFunction<String, Acc, Acc> {
        @Override public Acc createAccumulator() { return new Acc(); }

        @Override
        public Acc add(String msg, Acc acc) {
            try {
                JsonNode n = MAPPER.readTree(msg);
                acc.location = n.path("LOCATION_NAME").asText(acc.location);
                int outcome = n.path("ACTUAL_OUTCOME").asInt(-1);
                double error = n.path("PREDICTION_ERROR").asDouble(Double.NaN);
                double yesPrice = n.path("yes_price").asDouble(Double.NaN);
                boolean closed = n.path("market_closed").asBoolean(false);

                if (outcome < 0 || Double.isNaN(error)) return acc;

                acc.total++;
                if (outcome == 1) acc.correct++;
                acc.sumError += error;
                if (error > acc.maxError) acc.maxError = error;

                if (!Double.isNaN(yesPrice)) {
                    acc.sumSignedError += (yesPrice - outcome);
                }

                if (closed) {
                    acc.closedTotal++;
                    if (outcome == 1) acc.closedCorrect++;
                }
            } catch (Exception ignored) {}
            return acc;
        }

        @Override public Acc getResult(Acc acc) { return acc; }
        @Override public Acc merge(Acc a, Acc b) {
            a.total += b.total;
            a.correct += b.correct;
            a.closedTotal += b.closedTotal;
            a.closedCorrect += b.closedCorrect;
            a.sumError += b.sumError;
            a.sumSignedError += b.sumSignedError;
            if (b.maxError > a.maxError) a.maxError = b.maxError;
            if (a.location.isEmpty()) a.location = b.location;
            return a;
        }
    }

    // --- Window result function (attaches window metadata) ---

    static class WindowResultFunction
            extends ProcessWindowFunction<Acc, String, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Acc> elements, Collector<String> out) {
            Acc acc = elements.iterator().next();
            if (acc.total == 0) return;

            double accuracyRate = (double) acc.correct / acc.total;
            double avgError = acc.sumError / acc.total;
            double biasScore = acc.sumSignedError / acc.total;
            double closedAccuracy = acc.closedTotal > 0
                    ? (double) acc.closedCorrect / acc.closedTotal : Double.NaN;

            try {
                ObjectNode result = MAPPER.createObjectNode();
                result.put("LOCATION_NAME", acc.location);
                result.put("window_start", Instant.ofEpochMilli(ctx.window().getStart()).toString());
                result.put("window_end", Instant.ofEpochMilli(ctx.window().getEnd()).toString());
                result.put("total_count", acc.total);
                result.put("correct_count", acc.correct);
                result.put("accuracy_rate", round(accuracyRate));
                result.put("avg_prediction_error", round(avgError));
                result.put("max_prediction_error", round(acc.maxError));
                result.put("bias_score", round(biasScore));
                result.put("closed_count", acc.closedTotal);
                result.put("closed_correct_count", acc.closedCorrect);
                if (!Double.isNaN(closedAccuracy)) {
                    result.put("closed_accuracy_rate", round(closedAccuracy));
                }
                result.put("LAST_UPDATED", Instant.now().toString());
                out.collect(MAPPER.writeValueAsString(result));
            } catch (Exception ignored) {}
        }
    }

    static double round(double v) {
        return Math.round(v * 10000.0) / 10000.0;
    }
}
