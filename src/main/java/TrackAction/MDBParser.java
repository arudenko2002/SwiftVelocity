package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;

public class MDBParser extends DoFn<Document,TableRow> {
    Boolean checkRecord(JSONObject obj) {
        Boolean result=false;
        if(!obj.keySet().contains("firstName")
                || !obj.keySet().contains("lastName")
                || !obj.keySet().contains("email")
                || !obj.keySet().contains("active")
                || (obj.keySet().contains("active")&&!obj.getBoolean("active"))
                || !obj.keySet().contains("applications")
                || obj.getJSONArray("applications")==null
                || (obj.getJSONArray("applications")!=null && obj.getJSONArray("applications").length()==0)
                ) return false;
        JSONArray apps = obj.getJSONArray("applications");
        for(int i=0; i< apps.length();i++) {
            JSONObject app = apps.getJSONObject(i);
            if(
                    app.keySet().contains("name")
                    && app.getString("name")!=null
                    && app.getString("name").contains("swiftTrends")
                    && app.keySet().contains("active")
                    && app.getBoolean("active")
                    && app.keySet().contains("followedArtists")
                    && app.getJSONArray("followedArtists")!=null
                    && app.getJSONArray("followedArtists").length()>0
                    && app.keySet().contains("subscriptions")
                    && app.getJSONArray("subscriptions")!=null
                    && app.getJSONArray("subscriptions").length()>0
                    ) {
                JSONArray subscriptions = app.getJSONArray("subscriptions");
                for(int j=0; j<subscriptions.length();j++) {
                    JSONObject subscription= subscriptions.getJSONObject(j);
                    if(subscription.keySet().contains("name")
                            && subscription.getString("name").equals("velocityEmail")
                            && (
                                (subscription.keySet().contains("yourArtists") && subscription.getBoolean("yourArtists"))
                                        ||
                                        (subscription.keySet().contains("yourLabels") && subscription.getBoolean("yourLabels"))
                                )
                      )
                    {
                                return true;
                    }
                }
            }
        }
        return result;
    }

    ArrayList<String> getFollowedArtistsLables(JSONArray followedArtists,JSONArray followedLabels,JSONArray subscriptions){
        ArrayList<String> r = new ArrayList<String>();
        for(int i=0; i<subscriptions.length();i++) {
            JSONObject subscription= subscriptions.getJSONObject(i);
            if(subscription.keySet().contains("name")
                    && subscription.getString("name").equals("velocityEmail")
                    && (
                    (subscription.keySet().contains("yourArtists") && subscription.getBoolean("yourArtists"))
                            ||
                            (subscription.keySet().contains("yourLabels") && subscription.getBoolean("yourLabels"))
                    )
            )
            {
               if(subscription.getBoolean("yourArtists")) {
                   for (int ii = 0; ii < followedArtists.length(); ii++) {
                       JSONObject jo = followedArtists.getJSONObject(ii);
                       r.add("artist,"+jo.getBigInteger("_id").intValue());
                   }
               }
               if(subscription.getBoolean("yourLabels")) {
                   for (int ii = 0; ii < followedLabels.length(); ii++) {
                       JSONObject jo = followedLabels.getJSONObject(ii);
                       r.add("label,"+jo.getString("_id")+","+jo.getString("name"));
                   }
               }
            }
        }



        return r;
    }
    ArrayList<String> getFollowedLabels(JSONArray followedLabels){
        ArrayList<String> r = new ArrayList<String>();
        for (int i = 0; i < followedLabels.length(); i++) {
            JSONObject jo = followedLabels.getJSONObject(i);
            r.add(jo.getString("name"));
        }
        return r;
    }
    ArrayList<String> getTerritories(JSONArray subscriptions){
        ArrayList<String> r = new ArrayList<String>();
        for(int i=0; i<subscriptions.length();i++) {
            JSONObject subscription= subscriptions.getJSONObject(i);
            if(subscription.keySet().contains("name")
                    && subscription.getString("name").equals("velocityEmail")
                    && (
                    (subscription.keySet().contains("yourArtists") && subscription.getBoolean("yourArtists"))
                            ||
                            (subscription.keySet().contains("yourLabels") && subscription.getBoolean("yourLabels"))
            )
                    )
            {
                r.add(subscription.getJSONObject("primaryTerritory").getString("id")+","+subscription.getJSONObject("primaryTerritory").getString("name"));
                if(subscription.getJSONObject("secondaryTerritory").keySet().contains("id")) {
                    r.add(subscription.getJSONObject("secondaryTerritory").getString("id")+","+subscription.getJSONObject("secondaryTerritory").getString("name"));
                }
            }
        }
        return r;
    }
    ArrayList<String> getPartners(JSONArray subscriptions){
        ArrayList<String> r = new ArrayList<String>();
        for(int i=0; i<subscriptions.length();i++) {
            JSONObject subscription= subscriptions.getJSONObject(i);
            if(subscription.keySet().contains("name")
                    && subscription.getString("name").equals("velocityEmail")
                    && (
                    (subscription.keySet().contains("yourArtists") && subscription.getBoolean("yourArtists"))
                            ||
                            (subscription.keySet().contains("yourLabels") && subscription.getBoolean("yourLabels"))
            )
                    )
            {
                JSONArray partners = subscription.getJSONArray("partners");
                for(int j=0; j< partners.length();j++) {
                    if(partners.getJSONObject(j).getBoolean("value")) {
                        String partner = partners.getJSONObject(j).getString("name");
                        r.add(partner);
                    }
                }
            }
        }
        return r;
    }
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        Document line = c.element();
        JSONObject obj = new JSONObject(line);
        if(checkRecord(obj))
        {
            String userId = line.get("_id").toString();
            String firstName = obj.getString("firstName");
            String lastName = obj.getString("lastName");
            String email = obj.getString("email");
            JSONArray apps = obj.getJSONArray("applications");
            for(int i=0; i< apps.length();i++) {
                JSONObject app = apps.getJSONObject(i);
                if (
                        app.keySet().contains("name")
                                && app.getString("name") != null
                                && app.getString("name").contains("swiftTrends")) {
                    //JSONObject o = (JSONObject) obj.getJSONArray("applications").getJSONObject(0);
                    JSONObject application = apps.getJSONObject(i);
                    ArrayList<String> followedArtistsLabels =
                            getFollowedArtistsLables(application.getJSONArray("followedArtists"),
                            application.getJSONArray("followedLabels"),
                                    application.getJSONArray("subscriptions"));
                    //ArrayList<String> followedLabels = getFollowedLabels(application.getJSONArray("followedLabels"));
                    ArrayList<String> territories = getTerritories(application.getJSONArray("subscriptions"));
                    ArrayList<String> partners = getPartners(application.getJSONArray("subscriptions"));
                    for (int j = 0; j < followedArtistsLabels.size(); j++) {
                        String artist_label = followedArtistsLabels.get(j);
                        String[] artist_or_label = artist_label.split(",");
                        for(int k=0; k<partners.size();k++) {
                            String partner = partners.get(k);
                            TableRow tr = new TableRow();
                            tr.set("userId", userId);
                            tr.set("firstName", firstName);
                            tr.set("lastName", lastName);
                            tr.set("email", email);
                            if(artist_or_label[0].equals("artist")) {
                                tr.set("category", "artist");
                                tr.set("artist", artist_or_label[1]);
                            }
                            if(artist_or_label[0].equals("label")) {
                                tr.set("category", "label");
                                tr.set("label", artist_or_label[1]);
                                tr.set("labelname", artist_or_label[2]);
                            }
                            tr.set("primaryTerritoryCode",territories.get(0).split(",")[0]);
                            tr.set("primaryTerritoryName",territories.get(0).split(",")[1]);
                            if(territories.size()>1) {
                                tr.set("secondaryTerritoryCode", territories.get(1).split(",")[0]);
                                tr.set("secondaryTerritoryName", territories.get(1).split(",")[1]);
                            }
                            tr.set("partner",partner);
                            c.output(tr);
                            //System.out.println(tr.get("artist"));
                            System.out.println(userId + "," + firstName + "," + lastName + "," + email + "," + artist_label+","
                                    +territories.get(0)+","+territories.get(1)+","+partner);
                        }
                    }
                }
            }
        }
    }
}
