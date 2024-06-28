import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CloudBoltReport extends BaseRichBolt {
    private String output;
    private String filename;
    private BufferedWriter writer;
    private OutputCollector collector;

    public CloudBoltReport(String filename) {
        this.filename = filename;
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            // Open the file for writing
            //NOTE: SHOULD BE CHANGED IF NEEDED
            writer = new BufferedWriter(new FileWriter("/s/chopin/b/grad/swagata/cs535/PA2/PA2/Task_2"+filename+".txt", true)); // Append mode
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields, since this bolt doesn't emit any tuples
    }

    @Override
    public void execute(Tuple tuple) {
        String output = tuple.getStringByField("output");

        try {
            writer.write(output);
            writer.newLine();
            writer.flush(); // Flush to ensure data is written immediately
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        // Close the file writer when the bolt shuts down
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
