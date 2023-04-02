package com.mahoni.flink;

import com.mahoni.schema.AirQualityRawSchema;
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

    public static class AqiO3
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple3<String,Integer,String>, String, TimeWindow> {
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple3<String,Integer,String>> out) throws Exception {
            double o3Sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double o3 = Math.floor(air.getO3() * 1000) / 1000; //truncate 3 decimal
                o3Sum += o3;
                count ++;
            }
            //AqiMeasurement aqiMeasurement = new AqiMeasurement();
            double o3Avg = Math.floor(o3Sum / count * 1000) / 1000;
            Tuple2<Integer,String>  aqi = new Tuple2<>();
            if( o3Avg >= 0.125 && o3Avg <= 0.164 ){
                aqi = AqiMeasurement.calculateAqi(3,0.125,0.164, o3Avg);
            } else if (o3Avg >= 0.165 && o3Avg <= 0.204) {
                aqi = AqiMeasurement.calculateAqi(4,0.165,0.204, o3Avg);
            } else if (o3Avg >= 0.205 && o3Avg <= 0.404) {
                aqi = AqiMeasurement.calculateAqi(5,0.205,0.404, o3Avg);
            } else if (o3Avg >= 0.405 && o3Avg <= 0.504) {
                aqi = AqiMeasurement.calculateAqi(6,0.405,0.504, o3Avg);
            } else if (o3Avg >= 0.505 && o3Avg <= 0.604) {
                aqi = AqiMeasurement.calculateAqi(7,0.505,0.604, o3Avg);
            }

            out.collect(Tuple3.of(key, aqi.f0,aqi.f1));
        }
    }

    public static class AqiSO2
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple3<String,Integer,String>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple3<String,Integer,String>> out) throws Exception {
            double so2Sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                int so2 = (int)Math.round(air.getSo2()); //truncate integer
                so2Sum += so2;
                count ++;
            }
            double so2Avg = Math.floor(so2Sum / count * 1000) / 1000;
            Tuple2<Integer,String>  aqi = new Tuple2<>();
            if (so2Avg >= 0 && so2Avg <= 35){
                aqi = AqiMeasurement.calculateAqi(1,0,35, so2Avg);
            } else if (so2Avg >= 36 && so2Avg <= 75) {
                aqi = AqiMeasurement.calculateAqi(2,36,75, so2Avg);
            } else if( so2Avg >= 76 && so2Avg <= 185 ){
                aqi = AqiMeasurement.calculateAqi(3,76,185, so2Avg);
            }
            out.collect(Tuple3.of(key,aqi.f0,aqi.f1));
        }
    }

    public static class AqiNO2
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple3<String,Integer,String>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple3<String,Integer,String>> out) throws Exception {
            double no2Sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double no2 = Math.round(air.getNo2()); //truncate integer
                no2Sum += no2;
                count ++;
            }
            double no2Avg = Math.floor(no2Sum / count * 1000) / 1000;
            Tuple2<Integer,String>  aqi = new Tuple2<>();
            if (no2Avg >= 0 && no2Avg <= 53) {
                aqi = AqiMeasurement.calculateAqi(1,0,53, no2Avg);
            } else if (no2Avg >= 54 && no2Avg <= 100) {
                aqi = AqiMeasurement.calculateAqi(2,54,100, no2Avg);
            } else if( no2Avg >= 101 && no2Avg <= 360 ){
                aqi = AqiMeasurement.calculateAqi(3,101,360, no2Avg);
            } else if (no2Avg >= 361 && no2Avg <= 649) {
                aqi = AqiMeasurement.calculateAqi(4,361,649, no2Avg);
            } else if (no2Avg >= 650 && no2Avg <= 1249) {
                aqi = AqiMeasurement.calculateAqi(5,650,1249, no2Avg);
            } else if (no2Avg >= 1250 && no2Avg <= 1649) {
                aqi = AqiMeasurement.calculateAqi(6,1250,1649, no2Avg);
            } else if (no2Avg >= 1650 && no2Avg <= 2049) {
                aqi = AqiMeasurement.calculateAqi(7,1650,2049, no2Avg);
            }
            out.collect(Tuple3.of(key,aqi.f0,aqi.f1));
        }
    }

    public static class AqiCO
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple3<String,Integer,String>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple3<String,Integer,String>> out) throws Exception {
            double coSum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double co = Math.round(air.getCo()); //truncate integer
                coSum += co;
                count ++;
            }
            double coAvg = Math.floor(coSum / count * 10) / 10;
            Tuple2<Integer,String>  aqi = new Tuple2<>();
            if (coAvg >= 0 && coAvg <= 4.4) {
                aqi = AqiMeasurement.calculateAqi(1,0,4.4, coAvg);
            } else if (coAvg >= 4.5 && coAvg <= 9.4) {
                aqi = AqiMeasurement.calculateAqi(2,4.5,9.4, coAvg);
            } else if( coAvg >= 9.5 && coAvg <= 12.4 ){
                aqi = AqiMeasurement.calculateAqi(3,9.5,12.4, coAvg);
            } else if (coAvg >= 12.5 && coAvg <= 15.4) {
                aqi = AqiMeasurement.calculateAqi(4,12.5,15.4, coAvg);
            } else if (coAvg >= 15.5 && coAvg <= 30.4) {
                aqi = AqiMeasurement.calculateAqi(5,15.5,30.4, coAvg);
            } else if (coAvg >= 30.5 && coAvg <= 40.4) {
                aqi = AqiMeasurement.calculateAqi(6,30.5,40.4, coAvg);
            } else if (coAvg >= 40.5 && coAvg <= 50.4) {
                aqi = AqiMeasurement.calculateAqi(7,40.5,50.4, coAvg);
            }
            out.collect(Tuple3.of(key,aqi.f0,aqi.f1));
        }
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
            }

        }
    }
}
