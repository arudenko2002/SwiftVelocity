package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.DecimalFormat;
import java.util.Iterator;

public class ParDoMakeJSON extends DoFn<KV<String,Iterable<TableRow>>,String> {

    private String formatInteger(String number) {
        DecimalFormat df2 = new DecimalFormat( "#,###,###,##0" );
        try {
            int i = Integer.parseInt(number);
            return df2.format(i);
        } catch(Exception e) {
            System.out.println("I cannot parse 'followers'!!!");
            e.printStackTrace();
            return "0,000";
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String report_date="";
        JSONObject user = new JSONObject();
        JSONObject territory = new JSONObject();
        String partner="";
        JSONArray isrcs = new JSONArray();
        JSONObject alert = new JSONObject();

        KV<String, Iterable<TableRow>> pair = c.element();
        Iterator<TableRow> rows = pair.getValue().iterator();
        int counter=0;
        while(rows.hasNext()) {
            counter++;

            //Limit on 10 ISRCs per alert {user-partner-territory-[ISRCs]}
            if(counter>10) break;

            TableRow tr = rows.next();
            JSONObject record = new JSONObject(tr);
            if(counter==1) {
                report_date = record.getString("report_date");
                user.put("firstname",record.getString("firstname"));
                user.put("lastname",record.getString("lastname"));
                user.put("email",record.getString("email"));
                territory.put("country_code",record.getString("country_code"));
                territory.put("country_name",record.getString("country_name"));
                partner=record.getString("partner");
            }
            JSONObject isrc_info = new JSONObject();
            isrc_info.put("isrc",record.getString("isrc").replace("\n", ""));
            isrc_info.put("canopus_id",record.getBigInteger("canopus_id"));
            isrc_info.put("resource_rollup_id",record.getBigInteger("resource_rollup_id"));
            isrc_info.put("track_artist",record.getString("track_artist").replace("\n", ""));
            isrc_info.put("track_title",record.getString("track_title").replace("\n", ""));
            isrc_info.put("country_code",record.getString("country_code").replace("\n", ""));
            isrc_info.put("country_name",record.getString("country_name").replace("\n", ""));
            isrc_info.put("streams_tw",record.getInt("streams_tw"));
            isrc_info.put("streams_lw",record.getInt("streams_lw"));
            isrc_info.put("streams_change",record.getInt("streams_change"));
            isrc_info.put("streams_collection_tw",record.getInt("streams_collection_tw"));
            isrc_info.put("streams_collection_lw",record.getInt("streams_collection_lw"));
            isrc_info.put("streams_collection_change",record.getInt("streams_collection_change"));
            isrc_info.put("streams_collection_change_perc",record.getDouble("streams_collection_change_perc"));
            isrc_info.put("streams_other_tw",record.getInt("streams_other_tw"));
            isrc_info.put("streams_other_lw",record.getInt("streams_other_lw"));
            isrc_info.put("streams_other_change",record.getInt("streams_other_change"));
            isrc_info.put("streams_other_change_perc",record.getDouble("streams_other_change_perc"));
            isrc_info.put("streams_artist_tw",record.getInt("streams_artist_tw"));
            isrc_info.put("streams_artist_lw",record.getInt("streams_artist_lw"));
            isrc_info.put("streams_artist_change",record.getInt("streams_artist_change"));
            isrc_info.put("streams_artist_change_perc",record.getDouble("streams_artist_change_perc"));
            isrc_info.put("streams_album_tw",record.getInt("streams_album_tw"));
            isrc_info.put("streams_album_lw",record.getInt("streams_album_lw"));
            isrc_info.put("streams_album_change",record.getInt("streams_album_change"));
            isrc_info.put("streams_album_change_perc",record.getDouble("streams_album_change_perc"));
            isrc_info.put("streams_search_tw",record.getInt("streams_search_tw"));
            isrc_info.put("streams_search_lw",record.getInt("streams_search_lw"));
            isrc_info.put("streams_search_change",record.getInt("streams_search_change"));
            isrc_info.put("streams_search_change_perc",record.getDouble("streams_search_change_perc"));
            isrc_info.put("streams_playlist_tw",record.getInt("streams_playlist_tw"));
            isrc_info.put("streams_playlist_lw",record.getInt("streams_playlist_lw"));
            isrc_info.put("streams_playlist_change",record.getInt("streams_playlist_change"));
            isrc_info.put("streams_playlist_change_perc",record.getDouble("streams_playlist_change_perc"));
            isrc_info.put("streams_undeveloped_playlist_tw",record.getInt("streams_undeveloped_playlist_tw"));
            isrc_info.put("streams_undeveloped_playlist_lw",record.getInt("streams_undeveloped_playlist_lw"));
            isrc_info.put("streams_undeveloped_playlist_change",record.getInt("streams_undeveloped_playlist_change"));
            isrc_info.put("streams_undeveloped_playlist_change_perc",record.getDouble("streams_undeveloped_playlist_change_perc"));
            isrc_info.put("collection_perc",record.getDouble("collection_perc"));
            isrc_info.put("playlist_perc",record.getDouble("playlist_perc"));
            isrc_info.put("rank_tw",record.getInt("rank_tw"));
            isrc_info.put("rank_lw",record.getInt("rank_lw"));
            isrc_info.put("rank_change",record.getInt("rank_change"));
            isrc_info.put("rank_adj_score",record.getDouble("rank_adj_score"));
            isrc_info.put("avg_velocity",record.getDouble("avg_velocity"));
            if(record.keySet().contains("track_image"))
                isrc_info.put("track_image",record.getString("track_image").replace("\n", ""));
            else
                isrc_info.put("track_image","No image");
            if(record.keySet().contains("labelname"))
                isrc_info.put("labelname",record.getString("labelname"));
            isrc_info.put("first_stream_date",record.getString("first_stream_date"));
            isrcs.put(isrc_info);
        }
        alert.put("report_date",report_date);
        alert.put("user",user);
        alert.put("territory",territory);
        alert.put("partner",partner);
        alert.put("isrcs",isrcs);
        String json=alert.toString();
        System.out.println("json="+json);
        c.output(json);
    }
}