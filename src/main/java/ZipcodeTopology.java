import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.file.Path;

public class ZipcodeTopology {
    public static void main(String[] args) throws Exception {
        boolean isLocal = false;
        boolean isParallel = false;

        for (String input: args) {
            if (input.equals("-l"))
                isLocal = true;
            if (input.equals("-p"))
                isParallel = true;
        }

        Path inputPath = Paths.get("input/uszips.csv");
        System.out.println("Reading zip codes from: " + inputPath.toString());

        List<String> zipCodes = new ArrayList<>();

        try {
                Files.lines(inputPath).skip(1).forEach(line -> {
                String[] parts = line.split(",");
                zipCodes.add(parts[0]);
            });
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        TopologyBuilder builder = new TopologyBuilder();

        Config conf = new Config();
//        conf.setDebug(true);

        if(isParallel){
            //Split the zip codes into half
            int halfOfZipCode = zipCodes.size()/2;
            List<String> firstHalf = new ArrayList<>();
            List<String> secondHalf = new ArrayList<>();

            for (int i = 0; i < halfOfZipCode; i++) {
                firstHalf.add(zipCodes.get(i));
            }

            for (int i = halfOfZipCode; i < zipCodes.size(); i++) {
                secondHalf.add(zipCodes.get(i));
            }

            builder.setSpout("cloud-spout-1", new CloudSpout(firstHalf));
            builder.setSpout("cloud-spout-2", new CloudSpout(secondHalf));

            builder.setBolt("cloud-bolt-1", new CloudBoltLossy()).shuffleGrouping("cloud-spout-1");
            builder.setBolt("cloud-bolt-2", new CloudBoltLossy()).shuffleGrouping("cloud-spout-2");

            builder.setBolt("cloud-bolt-report-1", new CloudBoltReport("file1")).shuffleGrouping("cloud-bolt-1");
            builder.setBolt("cloud-bolt-report-2", new CloudBoltReport("file2")).shuffleGrouping("cloud-bolt-2");

        }else{
            builder.setSpout("aq-spout", new AirQSpout(zipCodes));
            builder.setBolt("aq-bolt", new AirQBolt()).shuffleGrouping("aq-spout");
        }

        if (isLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ZipcodeTopology", conf, builder.createTopology());
            Utils.sleep(2000000);
            cluster.killTopology("ZipcodeTopology");
            cluster.shutdown();
        } else {
            System.out.println("************************IS NOT LOCAL*********************");
            conf.setNumWorkers(4);
            StormSubmitter.submitTopologyWithProgressBar("ZipcodeTopology", conf, builder.createTopology());
        }
    }
}