package com.mahoni.flink;

import com.mahoni.schema.AirQualityRawSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AqiMeasurement {
    public static int aqiO3OneHour;
    public int aqiSO2OneHour;
    public int aqiNO2OneHour;
    public int aqiO3EightHour;
    public int aqiCOEightHour;
    public int aqiPm25OneDay;
    public int aqiPm10OneDay;

    public AqiMeasurement(){
        this.aqiO3OneHour = 0;
        this.aqiSO2OneHour = 0;
        this.aqiNO2OneHour = 0;
        this.aqiO3EightHour = 0;
        this.aqiCOEightHour = 0;
        this.aqiPm25OneDay = 0;
        this.aqiPm10OneDay = 0;
    }

    public int getAqiO3OneHour() {
        return aqiO3OneHour;
    }

    public static void setAqiO3OneHour(int aqiO3OneHour) {
        AqiMeasurement.aqiO3OneHour = aqiO3OneHour;
    }

    public int getAqiSO2OneHour() {
        return aqiSO2OneHour;
    }

    public void setAqiSO2OneHour(int aqiSO2OneHour) {
        this.aqiSO2OneHour = aqiSO2OneHour;
    }

    public int getAqiNO2OneHour() {
        return aqiNO2OneHour;
    }

    public void setAqiNO2OneHour(int aqiNO2OneHour) {
        this.aqiNO2OneHour = aqiNO2OneHour;
    }

    public int getAqiO3EightHour() {
        return aqiO3EightHour;
    }

    public void setAqiO3EightHour(int aqiO3EightHour) {
        this.aqiO3EightHour = aqiO3EightHour;
    }

    public int getAqiCOEightHour() {
        return aqiCOEightHour;
    }

    public void setAqiCOEightHour(int aqiCOEightHour) {
        this.aqiCOEightHour = aqiCOEightHour;
    }

    public int getAqiPm25OneDay() {
        return aqiPm25OneDay;
    }

    public void setAqiPm25OneDay(int aqiPm25OneDay) {
        this.aqiPm25OneDay = aqiPm25OneDay;
    }

    public int getAqiPm10OneDay() {
        return aqiPm10OneDay;
    }

    public void setAqiPm10OneDay(int aqiPm10OneDay) {
        this.aqiPm10OneDay = aqiPm10OneDay;
    }

    public static Integer calculateAqi(int levelAqi, double lowerlimit, double upperLimit, double avgMeasurement){
        int upperAqi;
        int lowerAqi;

        switch (levelAqi){
            case 1:
                upperAqi = 50;
                lowerAqi = 0;
                break;
            case 2:
                upperAqi = 100;
                lowerAqi = 51;
                break;
            case 3:
                upperAqi = 150;
                lowerAqi = 101;
                break;
            case 4:
                upperAqi = 200;
                lowerAqi = 151;
                break;
            case 5:
                upperAqi = 300;
                lowerAqi = 201;
                break;
            case 6:
                upperAqi = 400;
                lowerAqi = 301;
                break;
            case 7:
                upperAqi = 500;
                lowerAqi = 401;
                break;
            default:upperAqi=0;lowerAqi=0;
        }

        int aqi = (int)Math.floor(((upperAqi-lowerAqi)/(upperLimit-lowerlimit))*(avgMeasurement-lowerlimit) + lowerAqi);
        return aqi;
    }

    public static class AqiO3
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple2<String,Integer>, String, TimeWindow> {

        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple2<String,Integer>> out) throws Exception {
            double o3sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double o3 = Math.floor(air.getO3() * 1000) / 1000; //truncate 3 decimal
                o3sum += o3;
                count ++;
            }
            //AqiMeasurement aqiMeasurement = new AqiMeasurement();
            double o3avg = Math.floor(o3sum/count * 1000) / 1000;
            int aqi;
            if( o3avg >= 0.125 && o3avg <= 0.164 ){
                aqi = AqiMeasurement.calculateAqi(3,0.125,0.164,o3avg);
            } else if (o3avg >= 0.165 && o3avg <= 0.204) {
                aqi = AqiMeasurement.calculateAqi(4,0.165,0.204,o3avg);
            } else if (o3avg >= 0.205 && o3avg <= 0.404) {
                aqi = AqiMeasurement.calculateAqi(5,0.205,0.404,o3avg);
            } else if (o3avg >= 0.405 && o3avg <= 0.504) {
                aqi = AqiMeasurement.calculateAqi(6,0.405,0.504,o3avg);
            } else if (o3avg >= 0.505 && o3avg <= 0.604) {
                aqi = AqiMeasurement.calculateAqi(7,0.505,0.604,o3avg);
            } else {
                aqi = 605;
            }

            //AqiMeasurement.setAqiO3OneHour(aqi);

            out.collect(Tuple2.of(key,aqi));
        }
    }

    public static class AqiSO2
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple2<String,Integer>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple2<String,Integer>> out) throws Exception {
            double so2sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                int so2 = (int)Math.round(air.getSo2()); //truncate integer
                so2sum += so2;
                count ++;
            }
            double so2avg = Math.floor(so2sum/count * 1000) / 1000;
            int aqi;
            if (so2avg >= 0 && so2avg <= 35){
                aqi = AqiMeasurement.calculateAqi(1,0,35,so2avg);
            } else if (so2avg >= 36 && so2avg <= 75) {
                aqi = AqiMeasurement.calculateAqi(2,36,75,so2avg);
            } else if( so2avg >= 76 && so2avg <= 185 ){
                aqi = AqiMeasurement.calculateAqi(3,76,185,so2avg);
            } else{
                aqi = 605;
            }
            out.collect(Tuple2.of(key,aqi));
        }
    }

    public static class AqiNO2
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple2<String,Integer>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple2<String,Integer>> out) throws Exception {
            double no2sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double no2 = Math.round(air.getNo2()); //truncate integer
                no2sum += no2;
                count ++;
            }
            double no2avg = Math.floor(no2sum/count * 1000) / 1000;
            int aqi;
            if (no2avg >= 0 && no2avg <= 53) {
                aqi = AqiMeasurement.calculateAqi(1,0,53,no2avg);
            } else if (no2avg >= 54 && no2avg <= 100) {
                aqi = AqiMeasurement.calculateAqi(2,54,100,no2avg);
            } else if( no2avg >= 101 && no2avg <= 360 ){
                aqi = AqiMeasurement.calculateAqi(3,101,360,no2avg);
            } else if (no2avg >= 361 && no2avg <= 649) {
                aqi = AqiMeasurement.calculateAqi(4,361,649,no2avg);
            } else if (no2avg >= 650 && no2avg <= 1249) {
                aqi = AqiMeasurement.calculateAqi(5,650,1249,no2avg);
            } else if (no2avg >= 1250 && no2avg <= 1649) {
                aqi = AqiMeasurement.calculateAqi(6,1250,1649,no2avg);
            } else if (no2avg >= 1650 && no2avg <= 2049) {
                aqi = AqiMeasurement.calculateAqi(7,1650,2049,no2avg);
            } else {
                aqi = 605;
            }
            out.collect(Tuple2.of(key,aqi));
        }
    }

    public static class AqiCO
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple2<String,Integer>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple2<String,Integer>> out) throws Exception {
            double cosum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double co = Math.round(air.getCo()); //truncate integer
                cosum += co;
                count ++;
            }
            double coavg = Math.floor(cosum/count * 10) / 10;
            int aqi;
            if (coavg >= 0 && coavg <= 4.4) {
                aqi = AqiMeasurement.calculateAqi(1,0,4.4,coavg);
            } else if (coavg >= 4.5 && coavg <= 9.4) {
                aqi = AqiMeasurement.calculateAqi(2,4.5,9.4,coavg);
            } else if( coavg >= 9.5 && coavg <= 12.4 ){
                aqi = AqiMeasurement.calculateAqi(3,9.5,12.4,coavg);
            } else if (coavg >= 12.5 && coavg <= 15.4) {
                aqi = AqiMeasurement.calculateAqi(4,12.5,15.4,coavg);
            } else if (coavg >= 15.5 && coavg <= 30.4) {
                aqi = AqiMeasurement.calculateAqi(5,15.5,30.4,coavg);
            } else if (coavg >= 30.5 && coavg <= 40.4) {
                aqi = AqiMeasurement.calculateAqi(6,30.5,40.4,coavg);
            } else if (coavg >= 40.5 && coavg <= 50.4) {
                aqi = AqiMeasurement.calculateAqi(7,40.5,50.4,coavg);
            } else {
                aqi = 605;
            }
            out.collect(Tuple2.of(key,aqi));
        }
    }

    public static class AqiPM25
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple2<String,Integer>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple2<String,Integer>> out) throws Exception {
            double pm25sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double pm25 = Math.round(air.getPm25()); //truncate integer
                pm25sum += pm25;
                count ++;
            }
            double pm25avg = Math.floor(pm25sum/count * 10) / 10;
            int aqi;
            if (pm25avg >= 0 && pm25avg <= 12) {
                aqi = AqiMeasurement.calculateAqi(1,0,12,pm25avg);
            } else if (pm25avg >= 12.1 && pm25avg <= 35.4) {
                aqi = AqiMeasurement.calculateAqi(2,12.1,35.4,pm25avg);
            } else if( pm25avg >= 35.5 && pm25avg <= 55.4 ){
                aqi = AqiMeasurement.calculateAqi(3,35.5,55.4,pm25avg);
            } else if (pm25avg >= 55.5 && pm25avg <= 150.4) {
                aqi = AqiMeasurement.calculateAqi(4,55.5,150.4,pm25avg);
            } else if (pm25avg >= 150.5 && pm25avg <= 250.4) {
                aqi = AqiMeasurement.calculateAqi(5,150.5,250.4,pm25avg);
            } else if (pm25avg >= 250.5 && pm25avg <= 350.4) {
                aqi = AqiMeasurement.calculateAqi(6,250.5,350.4,pm25avg);
            } else if (pm25avg >= 350.5 && pm25avg <= 500.4) {
                aqi = AqiMeasurement.calculateAqi(7,350.5,500.4,pm25avg);
            } else {
                aqi = 605;
            }
            out.collect(Tuple2.of(key,aqi));
        }
    }

    public static class AqiPM10
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple2<String,Integer>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple2<String,Integer>> out) throws Exception {
            double pm10sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double pm10 = Math.round(air.getPm10()); //truncate integer
                pm10sum += pm10;
                count ++;
            }
            double pm10avg = Math.round(pm10sum);
            int aqi;
            if (pm10avg >= 0 && pm10avg <= 54) {
                aqi = AqiMeasurement.calculateAqi(1,0,54,pm10avg);
            } else if (pm10avg >= 55 && pm10avg <= 154) {
                aqi = AqiMeasurement.calculateAqi(2,55,154,pm10avg);
            } else if( pm10avg >= 155 && pm10avg <= 254 ){
                aqi = AqiMeasurement.calculateAqi(3,155,254,pm10avg);
            } else if (pm10avg >= 255 && pm10avg <= 354) {
                aqi = AqiMeasurement.calculateAqi(4,255,354,pm10avg);
            } else if (pm10avg >= 355 && pm10avg <= 424) {
                aqi = AqiMeasurement.calculateAqi(5,355,424,pm10avg);
            } else if (pm10avg >= 425 && pm10avg <= 504) {
                aqi = AqiMeasurement.calculateAqi(6,425,504,pm10avg);
            } else if (pm10avg >= 505 && pm10avg <= 604) {
                aqi = AqiMeasurement.calculateAqi(7,505,604,pm10avg);
            } else {
                aqi = 605;
            }
            out.collect(Tuple2.of(key,aqi));
        }
    }
}
