import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CloudBoltLossy extends BaseRichBolt {
    private double SIGMA;
    private double EPSILON;
    private ScheduledExecutorService scheduler;
    private int bucketWidth;
    private Map<String, AbstractMap.SimpleEntry<Integer, Integer>> CloudCoverageCounts;
    private int currentBucket;
    private int numberProcessed;
    private OutputCollector collector;


    public CloudBoltLossy() {
        this.EPSILON = .01;
        this.SIGMA = .2;
        this.bucketWidth = (int) (1 / (EPSILON)); // epsilon
        this.CloudCoverageCounts = new HashMap();
        this.currentBucket = 1;
        this.numberProcessed = 0;
//        this.countCondition = 0;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleAtFixedRate(this::SortAndPrint, 1, 60, TimeUnit.SECONDS);

    }

    @Override
    public void execute(Tuple tuple) {
        String value = tuple.getStringByField("value");
        String region = tuple.getStringByField("region");

        String[] array = value.split(",");
//        System.out.println(region);

        for (String level : array) {
            String currentRegionAndVal = region + level;
            AbstractMap.SimpleEntry<Integer, Integer> countBucketPair = CloudCoverageCounts.getOrDefault(currentRegionAndVal, new AbstractMap.SimpleEntry<>(1, currentBucket-1));

            numberProcessed++;

            // Increment the first element of the tuple if the key existed
            if (CloudCoverageCounts.containsKey(currentRegionAndVal)) {
                countBucketPair = new AbstractMap.SimpleEntry<>(countBucketPair.getKey() + 1, countBucketPair.getValue());
            }

            CloudCoverageCounts.put(currentRegionAndVal, countBucketPair);

            if(numberProcessed % bucketWidth == 0)  {
                // If the count is less than or equal to the current bucket number minus one,
                // the item is considered infrequent and removed
                CloudCoverageCounts.entrySet().removeIf(entry -> entry.getValue().getKey() +  entry.getValue().getValue() <= currentBucket);
                currentBucket++;
            }
        }
    }

    private void SortAndPrint() {
        String output = "";
//        countCondition =0;

        Map<String, Integer> frequenciesDict = new HashMap<>();

        for (Map.Entry<String, AbstractMap.SimpleEntry<Integer, Integer>> entry : CloudCoverageCounts.entrySet()) {
            String region = entry.getKey();
            AbstractMap.SimpleEntry<Integer, Integer> pair = entry.getValue();
            int frequency = pair.getKey();
            if(frequency > ((this.SIGMA - this.EPSILON) * currentBucket)) {
                frequenciesDict.put(region, frequency);
            }
        }

        Map<String, Integer> level_1 = new HashMap<>();
        Map<String, Integer> level_2 = new HashMap<>();
        Map<String, Integer> level_3 = new HashMap<>();
        Map<String, Integer> level_4 = new HashMap<>();
        Map<String, Integer> level_5 = new HashMap<>();

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
        }

//        System.out.println("***************************************CLLLOOOOOUUUUUDSSSS IN LOSSY ***************************************************");

        output = getTop5Entries(level_1, 1,output);
        output = getTop5Entries(level_2, 2,output);
        output = getTop5Entries(level_3, 3,output);
        output = getTop5Entries(level_4, 4,output);
        output = getTop5Entries(level_5, 5,output);

//        System.out.println("******************************************************************************************");

//        System.out.println(stringBuilder.toString());
//        stringBuilder.append("\n");
        this.collector.emit(new Values(output)); //or call
    }

    public static String getTop5Entries(Map<String, Integer> originalMap, int condition, String out) {
        String timeStamp = Instant.now().toString();
        out += timeStamp+": ";
        List<Map.Entry<String, Integer>> list = new ArrayList<>(originalMap.entrySet());

        // Sort the list based on values
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
                // Sort in descending order
                return entry2.getValue().compareTo(entry1.getValue());
            }
        });

        out+=condition;

        for (int i = 0; i < Math.min(5, list.size()); i++) {
            Map.Entry<String, Integer> entry = list.get(i);
            out+=" <"+entry.getKey()+ "> <"+entry.getValue()+">";
        }
        return out+"\n";
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("output"));
    }

    @Override
    public void cleanup() {
        scheduler.shutdown();
    }
}