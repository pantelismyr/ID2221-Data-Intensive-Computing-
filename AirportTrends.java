package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.basics.RideCleansing;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Calendar;
import java.util.TimeZone;
import static org.apache.flink.quickstart.AirportTrends.JFKTerminal.NOT_A_TERMINAL;



public class AirportTrends {

    public enum JFKTerminal {
        TERMINAL_1(71436),
        TERMINAL_2(71688),
        TERMINAL_3(71191),
        TERMINAL_4(70945),
        TERMINAL_5(70190),
        TERMINAL_6(70686),
        NOT_A_TERMINAL(-1);

        int mapGrid;

        private JFKTerminal(int grid){
            this.mapGrid = grid;
        }

        public static JFKTerminal gridToTerminal(int grid){
            for(JFKTerminal terminal : values()){
                if(terminal.mapGrid == grid) {
                    return terminal;}
            }
            return NOT_A_TERMINAL;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        String DATA_PATH = "/Users/Pantelis/flink-java-project/nycTaxiRides.gz";
        int maxEventDelay = 60;          // Max event Delay in sec.
        int servingSpeed = 2000;


        // get the taxi ride data stream - Note: you got to change the path to your local data file
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(DATA_PATH, maxEventDelay, servingSpeed));


        DataStream<Tuple3<JFKTerminal, Integer, Integer>> airportRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new RideCleansing.NYCFilter())
                // match rides to Terminals (rides -> gridId -> Terminals)
                .map(new GridMatcher())
                // filter out rides that do not start or stop in a terminal
                .filter(new  FilterFunction<Tuple2<JFKTerminal, Boolean>>(){
                    @Override
                    public boolean filter(Tuple2<JFKTerminal, Boolean> airport) throws Exception {
                        return  airport.f0 != NOT_A_TERMINAL;
                    }
                })
                // partitions by Terminal
                .<KeyedStream<Tuple2<JFKTerminal, Boolean>, Tuple2<JFKTerminal, Boolean>>>keyBy(0,1)
                // build window
                .timeWindow(Time.minutes(60))
                // count ride events in window and convert TimeStamps(Long) to hour of the day(Integer)
                .apply(new RideCount())
                // build Global window
                .timeWindowAll(Time.minutes(60))
                // take only the most popular terminals (unique for every hour)
                .maxBy(1);

        // print the output
        airportRides.print();

        // run the cleansing pipeline
        env.execute("Most Popular Terminals per hour");

    }

    public static class GridMatcher implements MapFunction<TaxiRide, Tuple2<JFKTerminal, Boolean>> {
        @Override
        public Tuple2<JFKTerminal, Boolean> map(TaxiRide taxiRide) throws Exception{
            if(taxiRide.isStart){
                // get grid cell id from start location
                int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
                // get Terminal from start location
                JFKTerminal terminal = JFKTerminal.gridToTerminal(gridId);
                return new Tuple2<>(terminal, taxiRide.isStart);
            }
            else{
                // get grid cell id from end location
                int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
                // get Terminal from end location
                JFKTerminal terminal = JFKTerminal.gridToTerminal(gridId);
                return new Tuple2<>(terminal, taxiRide.isStart);

            }
        }
    }

    public static class RideCount implements WindowFunction<
            Tuple2<JFKTerminal, Boolean>, 							// input type
            Tuple3<JFKTerminal, Integer, Integer>, 			        // output type
            Tuple, 												    // key type
            TimeWindow>                                             // window type
    {

        @SuppressWarnings("unchecked")
        @Override
        public void apply(
                Tuple key,
                TimeWindow window,
                Iterable<Tuple2<JFKTerminal, Boolean>> values,
                Collector<Tuple3<JFKTerminal, Integer, Integer>> out) throws Exception{

            //int cellId = ((Tuple2<Integer, Boolean>)key).f0;
            JFKTerminal terminal = ((Tuple2<JFKTerminal, Boolean>)key).f0;
            long windowTime = window.getEnd();
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            calendar.setTimeInMillis(windowTime);
            int hour = calendar.get(Calendar.HOUR_OF_DAY);

            int cnt = 0;
            for(Tuple2<JFKTerminal, Boolean> v : values){
                cnt += 1;
            }

            out.collect(new Tuple3<>(terminal, cnt, hour));


        }
    }

}