package com.mahoni.flink.air_quality.job;

import com.mahoni.flink.schema.AirQualityRawSchema;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Cleaning {
    public static class CleanAqi extends ProcessFunction<AirQualityRawSchema, AirQualityRawSchema> {
        @Override
        public void processElement(AirQualityRawSchema airQualityRawSchema,
                                   Context context,
                                   Collector<AirQualityRawSchema> out) throws Exception {
            double co = Math.floor(airQualityRawSchema.getCo() * 100) / 100;
            airQualityRawSchema.setCo(co);

            double no = Math.floor(airQualityRawSchema.getNo() * 100) / 100;
            airQualityRawSchema.setNo(no);

            double no2 = Math.floor(airQualityRawSchema.getNo2() * 100) / 100;
            airQualityRawSchema.setNo2(no2);

            double o3 = Math.floor(airQualityRawSchema.getO3() * 100) / 100;
            airQualityRawSchema.setO3(o3);

            double so2 = Math.floor(airQualityRawSchema.getSo2() * 100) / 100;
            airQualityRawSchema.setSo2(so2);

            double pm25 = Math.floor(airQualityRawSchema.getPm25() * 100) / 100;
            airQualityRawSchema.setPm25(pm25);

            double pm10 = Math.floor(airQualityRawSchema.getPm10() * 100) / 100;
            airQualityRawSchema.setPm10(pm10);

            double pm1 = Math.floor(airQualityRawSchema.getPm1() * 100) / 100;
            airQualityRawSchema.setPm1(pm1);

            double nh3 = Math.floor(airQualityRawSchema.getNh3() * 100) / 100;
            airQualityRawSchema.setNh3(nh3);

            double pressure = Math.floor(airQualityRawSchema.getPressure() * 100) / 100;
            airQualityRawSchema.setPressure(pressure);

            double humidity = Math.floor(airQualityRawSchema.getHumidity() * 100) / 100;
            airQualityRawSchema.setHumidity(humidity);

            out.collect(airQualityRawSchema);
        }
    }
}
