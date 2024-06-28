import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class CloudSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<String> zipCodes;
    private int index = 0;

    public CloudSpout(List<String> zipCodes) {
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

                String urlString = String.format("http://api.weatherapi.com/v1/forecast.json?key=70d7f426029744f6969225916242103&q=%s&days=3&aqi=no&alerts=no", zipCode);
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
//                    System.out.println(response);

                    StringBuilder stringBuilder = new StringBuilder();
                    JSONArray forecastOBJ = new JSONObject(response.toString()).getJSONObject("forecast").getJSONArray("forecastday");

                    for (int i = 0; i <= 2; i++) { // Outer loop from 0 to 2
                        JSONArray forecastHour = forecastOBJ.getJSONObject(i).getJSONArray("hour");
                        for (int j = 0; j <= 23; j++) { // Inner loop from 0 to 23

                            // Your code here
                            int hourCloudCoverage = forecastHour.getJSONObject(j).getInt("cloud");
                            int cloudLevel = (hourCloudCoverage / 20) + 1;
                            if (hourCloudCoverage == 100) {
                                cloudLevel = 5;
                            }
                            stringBuilder.append(cloudLevel + ",");
                        }
                    }

                    String region = extractRegion(response.toString());
                    String cloud = stringBuilder.toString();

                    this.collector.emit(new Values(region, cloud)); //or call
                } else {
                    System.out.println("No data for zip code: " + zipCode);
                }
            } catch (Exception e) {
                System.out.println("Error fetching weather data: " + e.getMessage());
                e.printStackTrace();
            }
            index++;
        }

    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("region", "value"));
    }

    public String extractRegion(String msgId) {
        // implement json parsing to extract UV values
        String location = new JSONObject(msgId).getJSONObject("location").getString("region");
        return location;
    }
}


