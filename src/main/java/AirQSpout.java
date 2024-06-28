import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class AirQSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<String> zipCodes;
    private int index = 0;

    public AirQSpout(List<String> zipCodes) {
        this.zipCodes = zipCodes;
        System.out.println("Length of Zipcode: " + zipCodes.size());
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        String zipCode = zipCodes.get(index);

        if (index < zipCodes.size()) {
            try {
                String urlString = String.format("http://api.weatherapi.com/v1/current.json?key=70d7f426029744f6969225916242103&q=%s&aqi=yes", zipCode);
                URL url = new URL(urlString);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                    String inputLine;
                    StringBuffer response = new StringBuffer();
                    while ((inputLine = reader.readLine()) != null) {
                        response.append(inputLine);
                    }
                    reader.close();


                    int aq = extractAirQualityLevel(response.toString());
                    String region = extractRegion(response.toString());

                    System.out.println("***********************API CALLS*********************");
                    System.out.println(region+":"+aq);
                    System.out.println("***********************API CALLS*********************");




                    this.collector.emit(new Values(region, aq)); //or call
                } else {
                    System.out.println("No data for zip code: " + zipCode);
                }
            } catch (Exception e) {
                System.out.println("Error fetching weather data: " + e.getMessage());
                e.printStackTrace();
            }
            index++;
        }else{
            System.out.println("NO ZIPS TO LOOK AT");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("region", "value"));
    }

    public int extractAirQualityLevel(String msgId) {
        // implement json parsing to extract UV values
        JSONObject airQuality = new JSONObject(msgId).getJSONObject("current").getJSONObject("air_quality");
        return airQuality.getInt("us-epa-index");
    }
     public String extractRegion(String msgId) {
         // implement json parsing to extract UV values
         String location = new JSONObject(msgId).getJSONObject("location").getString("region");
         return location;
     }
}

