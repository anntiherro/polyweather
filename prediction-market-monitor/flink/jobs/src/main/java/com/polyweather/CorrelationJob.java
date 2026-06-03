package com.polyweather;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CorrelationJob {

    static final ObjectMapper MAPPER = new ObjectMapper();
    static final String BOOTSTRAP = "kafka:29092";
    static final Pattern TEMP_PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)\\s*°?C");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);

        KafkaSource<String> polySource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("polymarket-predictions-raw")
                .setGroupId("correlation-job-poly")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        KafkaSource<String> weatherSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("weather-actuals-raw")
                .setGroupId("correlation-job-weather")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<String> polyStream = env.fromSource(polySource, WatermarkStrategy.noWatermarks(), "polymarket");
        DataStream<String> weatherStream = env.fromSource(weatherSource, WatermarkStrategy.noWatermarks(), "weather");

        // Key both streams by LOCATION_NAME and union for stateful processing
        DataStream<String> correlations = polyStream
                .keyBy(msg -> extractField(msg, "LOCATION_NAME"))
                .connect(weatherStream.keyBy(msg -> extractField(msg, "LOCATION_NAME")))
                .flatMap(new CorrelationFunction());

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("market-weather-correlations")
                        .setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                        .build())
                .build();

        correlations.sinkTo(sink);
        env.execute("Polymarket Weather Correlation");
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

    public static class CorrelationFunction
            extends org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction<String, String, String> {

        private ValueState<String> latestWeather;

        @Override
        public void open(OpenContext openContext) throws Exception {
            latestWeather = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("latest-weather", Types.STRING));
        }

        @Override
        public void flatMap1(String polyMsg, Collector<String> out) throws Exception {
            String weatherJson = latestWeather.value();
            if (weatherJson == null) return;

            JsonNode poly = MAPPER.readTree(polyMsg);
            JsonNode weather = MAPPER.readTree(weatherJson);

            // Check if weather is recent (within 15 minutes)
            String weatherTs = weather.path("current").path("time").asText("");
            String pollTs = weather.path("POLL_TIMESTAMP").asText("");
            String refTs = !pollTs.isEmpty() ? pollTs : weatherTs;
            if (!refTs.isEmpty()) {
                Instant weatherTime = Instant.parse(refTs.endsWith("Z") ? refTs : refTs + "Z");
                if (Duration.between(weatherTime, Instant.now()).abs().toMinutes() > 15) return;
            }

            String question = poly.path("question").asText("");
            double actualTemp = weather.path("current").path("temperature_2m").asDouble(Double.NaN);
            if (Double.isNaN(actualTemp)) return;

            // Parse target temperature from question ("be 15°C", "be above 25°C", etc.)
            Matcher m = TEMP_PATTERN.matcher(question);
            if (!m.find()) return;
            double targetTemp = Double.parseDouble(m.group(1));

            // Determine if "Yes" outcome is correct based on question type
            boolean isAbove = question.toLowerCase().contains("above") || question.toLowerCase().contains("exceed");
            boolean isBelow = question.toLowerCase().contains("below") || question.toLowerCase().contains("under");
            boolean actualOutcome;
            if (isAbove) {
                actualOutcome = actualTemp > targetTemp;
            } else if (isBelow) {
                actualOutcome = actualTemp < targetTemp;
            } else {
                // Exact match within ±1°C tolerance
                actualOutcome = Math.abs(actualTemp - targetTemp) <= 1.0;
            }

            double yesPrice = 0.0;
            for (JsonNode token : poly.path("tokens")) {
                if ("Yes".equalsIgnoreCase(token.path("outcome").asText())) {
                    yesPrice = token.path("price").asDouble(0);
                    break;
                }
            }

            double predictionError = Math.abs(yesPrice - (actualOutcome ? 1.0 : 0.0));

            String polyTs = poly.path("POLL_TIMESTAMP").asText("");
            long latencySeconds = 0;
            if (!polyTs.isEmpty() && !refTs.isEmpty()) {
                try {
                    Instant t1 = Instant.parse(polyTs.endsWith("Z") ? polyTs : polyTs + "Z");
                    Instant t2 = Instant.parse(refTs.endsWith("Z") ? refTs : refTs + "Z");
                    latencySeconds = Math.abs(Duration.between(t1, t2).getSeconds());
                } catch (Exception ignored) {}
            }

            ObjectNode result = MAPPER.createObjectNode();
            result.put("condition_id", poly.path("condition_id").asText());
            result.put("question", question);
            result.put("LOCATION_NAME", poly.path("LOCATION_NAME").asText());
            result.put("yes_price", yesPrice);
            result.put("target_temp", targetTemp);
            result.put("actual_temp", actualTemp);
            result.put("ACTUAL_OUTCOME", actualOutcome ? 1 : 0);
            result.put("PREDICTION_ERROR", Math.round(predictionError * 10000.0) / 10000.0);
            result.put("CORRELATION_LATENCY_SEC", latencySeconds);
            result.put("arbitrage_flag", poly.path("arbitrage_flag").asBoolean(false));
            result.put("POLL_TIMESTAMP", polyTs);

            out.collect(MAPPER.writeValueAsString(result));
        }

        @Override
        public void flatMap2(String weatherMsg, Collector<String> out) throws Exception {
            // Update latest weather for this location
            latestWeather.update(weatherMsg);
        }
    }
}
