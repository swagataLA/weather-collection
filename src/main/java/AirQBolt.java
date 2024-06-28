//import jdk.internal.util.xml.impl.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AirQBolt extends BaseRichBolt {
    private double SIGMA;
    private double EPSILON;
    private ScheduledExecutorService scheduler;
    private int bucketWidth;
    private Map<String, SimpleEntry<Integer, Integer>> airQualityCounts;
    private int currentBucket;
    private int numberProcessed;
    static StringBuilder stringBuilder;
    private BufferedWriter writer;


    public AirQBolt() {
        this.EPSILON = .01;
        this.SIGMA = .2;
        this.bucketWidth = (int) (1 / (EPSILON)); // epsilon
        this.airQualityCounts = new HashMap();
        this.currentBucket = 0;
        this.numberProcessed = 0;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleAtFixedRate(this::SortAndPrint, 1, 15, TimeUnit.SECONDS);
        try {
            // Open the file for writing
            //NOTE: SHOULD BE CHANGED IF NEEDED TO SPECIFY THE NAME OF THE FILE
            writer = new BufferedWriter(new FileWriter("/s/chopin/b/grad/swagata/cs535/PA2/PA2/Task_1.txt", true)); // Append mode
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        numberProcessed++;

        int value = tuple.getIntegerByField("value");
        String region = tuple.getStringByField("region");

        //Add to the bucket
        String currentRegionAndVal = region+value;
        SimpleEntry<Integer, Integer> countBucketPair = airQualityCounts.getOrDefault(currentRegionAndVal, new SimpleEntry<>(1, currentBucket));

        // Increment the first element of the tuple if the key existed
        if (airQualityCounts.containsKey(currentRegionAndVal)) {
            countBucketPair = new SimpleEntry<>(countBucketPair.getKey() + 1, countBucketPair.getValue());
        }

        airQualityCounts.put(currentRegionAndVal, countBucketPair);

        if(numberProcessed % bucketWidth == 0)  {
            // If the count is less than or equal to the current bucket number minus one,
            // the item is considered infrequent and removed
            airQualityCounts.entrySet().removeIf(entry -> entry.getValue().getKey() +  entry.getValue().getValue() <= currentBucket);
            currentBucket++;
        }
    }

    private void SortAndPrint() {
        stringBuilder = new StringBuilder();

        Map<String, Integer> frequenciesDict = new HashMap<>();


        for (Map.Entry<String, SimpleEntry<Integer, Integer>> entry : airQualityCounts.entrySet()) {
            String region = entry.getKey();
            SimpleEntry<Integer, Integer> pair = entry.getValue();
            int frequency = pair.getKey();
            //EXTRA PRUNING
            if(frequency > ((this.SIGMA - this.EPSILON) * currentBucket)){
                frequenciesDict.put(region, frequency);
            }
        }

        //return the list the frequency pruned even more

        Map<String, Integer> level_1 = new HashMap<>();
        Map<String, Integer> level_2 = new HashMap<>();
        Map<String, Integer> level_3 = new HashMap<>();
        Map<String, Integer> level_4 = new HashMap<>();
        Map<String, Integer> level_5 = new HashMap<>();
        Map<String, Integer> level_6 = new HashMap<>();

        for (Map.Entry<String, Integer> entry : frequenciesDict.entrySet()) {
            String region = entry.getKey();
            int frequency = entry.getValue();
            if(region.contains("1"))
                level_1.put(region.substring(0,region.length()-1),frequency);
            if(region.contains("2"))
                level_2.put(region.substring(0,region.length()-1),frequency);
            if(region.contains("3"))
                level_3.put(region.substring(0,region.length()-1),frequency);
            if(region.contains("4"))
                level_4.put(region.substring(0,region.length()-1),frequency);
            if(region.contains("5"))
                level_5.put(region.substring(0,region.length()-1),frequency);
            if(region.contains("6"))
                level_6.put(region.substring(0,region.length()-1),frequency);
        }

        getTop5Entries(level_1, 1);
        getTop5Entries(level_2, 2);
        getTop5Entries(level_3, 3);
        getTop5Entries(level_4, 4);
        getTop5Entries(level_5, 5);
        getTop5Entries(level_6, 6);

        try {
            writer.write(stringBuilder.toString());
            writer.newLine();
            writer.flush(); // Flush to ensure data is written immediately
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getTop5Entries(Map<String, Integer> originalMap, int condition) {
        String timeStamp = Instant.now().toString();
        stringBuilder.append(timeStamp);

        // Convert map to a list of entries
        List<Map.Entry<String, Integer>> list = new ArrayList<>(originalMap.entrySet());

        // Sort the list based on values
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
                // Sort in descending order
                return entry2.getValue().compareTo(entry1.getValue());
            }
        });

        stringBuilder.append(" "+ condition);

        for (int i = 0; i < Math.min(5, list.size()); i++) {
            Map.Entry<String, Integer> entry = list.get(i);
            stringBuilder.append(" "+entry.getKey()+ " "+entry.getValue());
        }
        stringBuilder.append("\n");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("output"));
    }

    @Override
    public void cleanup(){
        scheduler.shutdown();
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}