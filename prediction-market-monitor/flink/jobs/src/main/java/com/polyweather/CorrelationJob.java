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
    static final Pattern TEMP_PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)\\s*°?([CF])");

    enum QuestionType { HIGHEST, LOWEST, EXACT }

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
                .setGroupId("correlation-job-weather-v2")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<String> polyStream = env.fromSource(polySource, WatermarkStrategy.noWatermarks(), "polymarket");
        DataStream<String> weatherStream = env.fromSource(weatherSource, WatermarkStrategy.noWatermarks(), "weather");

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

    static QuestionType detectQuestionType(String question) {
        String q = question.toLowerCase();
        if (q.contains("highest") || q.contains("maximum") || q.contains("high temp")) {
            return QuestionType.HIGHEST;
        } else if (q.contains("lowest") || q.contains("minimum") || q.contains("low temp")) {
            return QuestionType.LOWEST;
        }
        return QuestionType.EXACT;
    }

    static boolean computeOutcome(String question, double actualTemp, double targetTemp) {
        String q = question.toLowerCase();
        if (q.contains("above") || q.contains("exceed") || q.contains("more than") || q.contains("at least")) {
            return actualTemp > targetTemp;
        } else if (q.contains("below") || q.contains("under") || q.contains("less than")) {
            return actualTemp < targetTemp;
        }
        return Math.abs(actualTemp - targetTemp) <= 1.0;
    }

    public static class CorrelationFunction
            extends org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction<String, String, String> {

        // Separate states for open-market forecasts and closed-market historical data
        private ValueState<String> latestForecastWeather;
        private ValueState<String> latestHistoricalWeather;

        @Override
        public void open(OpenContext openContext) throws Exception {
            latestForecastWeather = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("latest-forecast-weather", Types.STRING));
            latestHistoricalWeather = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("latest-historical-weather", Types.STRING));
        }

        @Override
        public void flatMap1(String polyMsg, Collector<String> out) throws Exception {
            JsonNode poly = MAPPER.readTree(polyMsg);
            boolean isClosed = poly.path("closed").asBoolean(false);
            String question = poly.path("question").asText("");

            String weatherJson = isClosed ? latestHistoricalWeather.value() : latestForecastWeather.value();
            if (weatherJson == null) return;

            JsonNode weather = MAPPER.readTree(weatherJson);
            String dataType = weather.path("data_type").asText("current");
            QuestionType questionType = detectQuestionType(question);

            // Select actual temperature based on question type and available data
            double actualTemp = Double.NaN;
            if ("forecast".equals(dataType) || "historical".equals(dataType)) {
                JsonNode daily = weather.path("daily");
                if (questionType == QuestionType.HIGHEST) {
                    actualTemp = daily.path("temperature_2m_max").asDouble(Double.NaN);
                } else if (questionType == QuestionType.LOWEST) {
                    actualTemp = daily.path("temperature_2m_min").asDouble(Double.NaN);
                } else {
                    // Exact questions: use current if available, else daily max
                    JsonNode current = weather.path("current");
                    if (!current.isMissingNode()) {
                        actualTemp = current.path("temperature_2m").asDouble(Double.NaN);
                    }
                    if (Double.isNaN(actualTemp)) {
                        actualTemp = daily.path("temperature_2m_max").asDouble(Double.NaN);
                    }
                }
            } else {
                // Legacy "current" records — enforce freshness check
                actualTemp = weather.path("current").path("temperature_2m").asDouble(Double.NaN);
                String pollTs = weather.path("POLL_TIMESTAMP").asText("");
                if (!pollTs.isEmpty()) {
                    Instant weatherTime = Instant.parse(pollTs.endsWith("Z") ? pollTs : pollTs + "Z");
                    if (Duration.between(weatherTime, Instant.now()).abs().toMinutes() > 15) return;
                }
            }

            if (Double.isNaN(actualTemp)) return;

            Matcher m = TEMP_PATTERN.matcher(question);
            if (!m.find()) return;
            double targetTemp = Double.parseDouble(m.group(1));
            if ("F".equals(m.group(2))) {
                targetTemp = (targetTemp - 32) * 5.0 / 9.0;
            }

            // Determine yes_price
            double yesPrice = 0.0;
            for (JsonNode token : poly.path("tokens")) {
                if ("Yes".equalsIgnoreCase(token.path("outcome").asText())) {
                    yesPrice = token.path("price").asDouble(0);
                    break;
                }
            }

            // For closed markets, use winner field as ground truth when available
            boolean actualOutcome;
            if (isClosed) {
                Boolean winnerKnown = null;
                for (JsonNode token : poly.path("tokens")) {
                    if ("Yes".equalsIgnoreCase(token.path("outcome").asText())) {
                        JsonNode winnerNode = token.path("winner");
                        if (winnerNode.isBoolean()) {
                            winnerKnown = winnerNode.asBoolean();
                        }
                        break;
                    }
                }
                actualOutcome = (winnerKnown != null) ? winnerKnown : computeOutcome(question, actualTemp, targetTemp);
            } else {
                actualOutcome = computeOutcome(question, actualTemp, targetTemp);
            }

            double predictionError = Math.abs(yesPrice - (actualOutcome ? 1.0 : 0.0));

            String polyTs = poly.path("POLL_TIMESTAMP").asText("");
            String weatherPollTs = weather.path("POLL_TIMESTAMP").asText("");
            long latencySeconds = 0;
            if (!polyTs.isEmpty() && !weatherPollTs.isEmpty()) {
                try {
                    Instant t1 = Instant.parse(polyTs.endsWith("Z") ? polyTs : polyTs + "Z");
                    Instant t2 = Instant.parse(weatherPollTs.endsWith("Z") ? weatherPollTs : weatherPollTs + "Z");
                    latencySeconds = Math.abs(Duration.between(t1, t2).getSeconds());
                } catch (Exception ignored) {}
            }

            ObjectNode result = MAPPER.createObjectNode();
            result.put("condition_id", poly.path("condition_id").asText());
            result.put("question", question);
            result.put("question_type", questionType.name());
            result.put("LOCATION_NAME", poly.path("LOCATION_NAME").asText());
            result.put("yes_price", yesPrice);
            result.put("target_temp", targetTemp);
            result.put("actual_temp", actualTemp);
            result.put("data_type", dataType);
            result.put("market_closed", isClosed);
            result.put("ACTUAL_OUTCOME", actualOutcome ? 1 : 0);
            result.put("PREDICTION_ERROR", Math.round(predictionError * 10000.0) / 10000.0);
            result.put("CORRELATION_LATENCY_SEC", latencySeconds);
            result.put("arbitrage_flag", poly.path("arbitrage_flag").asBoolean(false));
            result.put("POLL_TIMESTAMP", polyTs);

            out.collect(MAPPER.writeValueAsString(result));
        }

        @Override
        public void flatMap2(String weatherMsg, Collector<String> out) throws Exception {
            try {
                JsonNode weather = MAPPER.readTree(weatherMsg);
                String dataType = weather.path("data_type").asText("current");
                if ("historical".equals(dataType)) {
                    latestHistoricalWeather.update(weatherMsg);
                } else {
                    latestForecastWeather.update(weatherMsg);
                }
            } catch (Exception ignored) {}
        }
    }
}
