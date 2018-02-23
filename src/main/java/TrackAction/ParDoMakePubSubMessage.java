package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.DecimalFormat;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

public class ParDoMakePubSubMessage extends DoFn<String,PubsubMessage> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String message = c.element();
        JSONObject msg = new JSONObject(message);
        JSONObject user = msg.getJSONObject("user");
        Map attributes = new Hashtable();
        attributes.put("email",user.getString("email"));
        attributes.put("firstname",user.getString("firstname"));
        attributes.put("lastname",user.getString("lastname"));
        attributes.put("subject","Velocity Alerts");
        PubsubMessage pubsubMessage = new PubsubMessage(message.getBytes("UTF-8"),attributes);
        System.out.println("message"+pubsubMessage.toString());
        c.output(pubsubMessage);
    }
}
