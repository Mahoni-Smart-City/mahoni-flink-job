package com.mahoni.flink.air_quality.job;

import com.mahoni.flink.schema.AirQualityRawSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AqiMeasurement {

    public static Tuple2<Integer,String> calculateAqi(int levelAqi, double lowerlimit, double upperLimit, double avgMeasurement){
        int upperAqi;
        int lowerAqi;
        String category;

        switch (levelAqi){
            case 1:
                upperAqi = 50;
                lowerAqi = 0;
                category = "Good";
                break;
            case 2:
                upperAqi = 100;
                lowerAqi = 51;
                category = "Moderate";
                break;
            case 3:
                upperAqi = 150;
                lowerAqi = 101;
                category = "Unhealthy for Sensitive Groups";
                break;
            case 4:
                upperAqi = 200;
                lowerAqi = 151;
                category = "Unhealthy";
                break;
            case 5:
                upperAqi = 300;
                lowerAqi = 201;
                category = "Very unhealthy";
                break;
            case 6:
                upperAqi = 400;
                lowerAqi = 301;
                category = "Hazardous";
                break;
            case 7:
                upperAqi = 500;
                lowerAqi = 401;
                category = "Hazardous";
                break;
            default:
                upperAqi = 0;
                lowerAqi = 0;
                category = "";
        }

        int aqi = (int)Math.floor(((upperAqi - lowerAqi) / (upperLimit - lowerlimit)) * (avgMeasurement - lowerlimit) + lowerAqi);
        return Tuple2.of(aqi,category);
    }
    public static class AqiPM25
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple3<String,Integer,String>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple3<String,Integer,String>> out) throws Exception {
            double pm25Sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double pm25 = Math.round(air.getPm25()); //truncate integer
                pm25Sum += pm25;
                count ++;
            }
            double pm25Avg = Math.floor(pm25Sum / count * 10) / 10;
            Tuple2<Integer,String>  aqi = new Tuple2<>();
            if (pm25Avg >= 0 && pm25Avg <= 12) {
                aqi = AqiMeasurement.calculateAqi(1,0,12, pm25Avg);
            } else if (pm25Avg >= 12.1 && pm25Avg <= 35.4) {
                aqi = AqiMeasurement.calculateAqi(2,12.1,35.4, pm25Avg);
            } else if( pm25Avg >= 35.5 && pm25Avg <= 55.4 ){
                aqi = AqiMeasurement.calculateAqi(3,35.5,55.4, pm25Avg);
            } else if (pm25Avg >= 55.5 && pm25Avg <= 150.4) {
                aqi = AqiMeasurement.calculateAqi(4,55.5,150.4, pm25Avg);
            } else if (pm25Avg >= 150.5 && pm25Avg <= 250.4) {
                aqi = AqiMeasurement.calculateAqi(5,150.5,250.4, pm25Avg);
            } else if (pm25Avg >= 250.5 && pm25Avg <= 350.4) {
                aqi = AqiMeasurement.calculateAqi(6,250.5,350.4, pm25Avg);
            } else if (pm25Avg >= 350.5 && pm25Avg <= 500.4) {
                aqi = AqiMeasurement.calculateAqi(7,350.5,500.4, pm25Avg);
            } else {
                aqi = AqiMeasurement.calculateAqi(7,350.5,500.4, pm25Avg);
            }
            out.collect(Tuple3.of(key,aqi.f0,aqi.f1));
        }
    }

    public static class AqiPM10
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple3<String,Integer,String>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple3<String,Integer,String>> out) throws Exception {
            double pm10Sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double pm10 = Math.round(air.getPm10()); //truncate integer
                pm10Sum += pm10;
                count ++;
            }
            double pm10Avg = Math.round(pm10Sum);
            Tuple2<Integer,String>  aqi = new Tuple2<>();
            if (pm10Avg >= 0 && pm10Avg <= 54) {
                aqi = AqiMeasurement.calculateAqi(1,0,54, pm10Avg);
            } else if (pm10Avg >= 55 && pm10Avg <= 154) {
                aqi = AqiMeasurement.calculateAqi(2,55,154, pm10Avg);
            } else if( pm10Avg >= 155 && pm10Avg <= 254 ){
                aqi = AqiMeasurement.calculateAqi(3,155,254, pm10Avg);
            } else if (pm10Avg >= 255 && pm10Avg <= 354) {
                aqi = AqiMeasurement.calculateAqi(4,255,354, pm10Avg);
            } else if (pm10Avg >= 355 && pm10Avg <= 424) {
                aqi = AqiMeasurement.calculateAqi(5,355,424, pm10Avg);
            } else if (pm10Avg >= 425 && pm10Avg <= 504) {
                aqi = AqiMeasurement.calculateAqi(6,425,504, pm10Avg);
            } else if (pm10Avg >= 505 && pm10Avg <= 604) {
                aqi = AqiMeasurement.calculateAqi(7,505,604, pm10Avg);
            } else {
                aqi = AqiMeasurement.calculateAqi(7,505,604, pm10Avg);
            }
            out.collect(Tuple3.of(key,aqi.f0,aqi.f1));
        }
    }

    public static class SearchMaxAqi
            extends KeyedProcessFunction<String,Tuple3<String,Integer,String>, Tuple3<String,Integer,String>> {
        private MapState<String, Integer> maxValues;

        @Override
        public void open(Configuration config) {
            maxValues = getRuntimeContext().getMapState(new MapStateDescriptor<>("maxValues", String.class, Integer.class));
        }
        @Override
        public void processElement(Tuple3<String,Integer,String> aqi,
                                   Context context,
                                   Collector<Tuple3<String,Integer,String>> out) throws Exception {
            Integer currentValue = aqi.f1;
            String currentKey = aqi.f0;
            String category = aqi.f2;
            Integer currentMax = maxValues.get(currentKey);

            if (currentMax == null || currentValue > currentMax) {
                maxValues.put(currentKey, currentValue);
                out.collect(Tuple3.of(currentKey, currentValue,category));
                maxValues.put(currentKey, 0);
            }

        }
    }
}
