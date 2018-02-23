package TrackAction;

import com.google.cloud.bigquery.*;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.beam.sdk.io.TextIO;
import com.google.api.services.bigquery.model.TableRow;

public class SaveBQTableAsJson {
    public String project="umg-dev";
    public String runner="DirectRunner";
    //public String dataSet="swift_alerts";
    //public String tableName="playlist_track_action";
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
   private String fields = "userId,firstname,lastname,email,artist,label,labelname,primaryTerritoryCode,primaryTerritoryName,secondaryTerritoryCode,secondaryTerritoryName"+ //10
            ",partner,report_date,isrc,canopus_id,resource_rollup_id,track_artist,track_title,"+ //6
            "country_code,country_name,streams_tw,streams_lw,streams_change,streams_collection_tw,streams_collection_lw,streams_collection_change,"+  //8
            "streams_collection_change_perc,streams_other_tw,streams_other_lw,streams_other_change,streams_other_change_perc,streams_artist_tw,streams_artist_lw,"+  //7
            "streams_artist_change,streams_artist_change_perc,streams_album_tw,streams_album_lw,streams_album_change,streams_album_change_perc,streams_search_tw,"+ //7
            "streams_search_lw,streams_search_change,streams_search_change_perc,streams_playlist_tw,streams_playlist_lw,streams_playlist_change,"+  //6
            "streams_playlist_change_perc,streams_undeveloped_playlist_tw,streams_undeveloped_playlist_lw,streams_undeveloped_playlist_change,"+  //4
            "streams_undeveloped_playlist_change_perc,collection_perc,playlist_perc,rank_tw,rank_lw,rank_change,rank_adj_score,avg_velocity,track_image,first_stream_date";  //9
    //private String fields="firstname,email,artist_name,artist_uri,isrc,name,playlist_uri,owner_id,country,followers,position,action_type,product_title,max(track_uri) as track_uri,max(track_image) as track_image,max(artist_image) as artist_image,max('{actualDate}') as reportdate";
    public String whom="justtome";
    private String[] arguments=null;

    public SaveBQTableAsJson(String[] args) {
        arguments=args;
    }

    private String getArgument(String find) {
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals(find))return arguments[i+1];
        }
        return "Not found";
    }

    void executeQueueGeneral(String executionDate) throws Exception{
        String actualDate=getDaysAgo(executionDate,-1);
        String source=getArgument("--velocity_alerts");
        String query ="SELECT "+fields.replace("{actualDate}",actualDate)+" FROM `"+source+"` WHERE _PARTITIONTIME = TIMESTAMP('"+actualDate+
                "') GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12"+
                ",13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60"+
                " ORDER BY email,partner,country_code,rank_adj_score DESC";
        executePipe(query,actualDate);
    }

    void executePipe(String sql, String actualDate) {
        System.out.println(sql);
        String[] args={"--project="+getArgument("--project"),"--runner="+runner};
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation(getArgument("--temp_directory"));
        // Create the Pipeline object with the options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        BigQueryIO.Read readBigQuery = BigQueryIO.read().fromQuery(sql).usingStandardSql();
        pipeline.apply(readBigQuery)
                .apply(ParDo.of(new MapWithKey()))
                .apply(GroupByKey.<String, TableRow>create())
                .apply(ParDo.of(new ParDoMakeJSON()))
                .apply(ParDo.of(new ParDoMakePubSubMessage()))
                .apply(PubsubIO.writeMessages().to(getArgument("--topic")))
        ;
        pipeline.run().waitUntilFinish();
    }

    public String getToday() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public String getDaysAgo(String day, int daysago) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        if (day!=null) {
            date = dateFormat.parse(day);
        }
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, daysago);
        Date newDate = cal.getTime();
        String newDateStr = dateFormat.format(newDate);
        return newDateStr;
    }

    public static void main(String[] args) throws Exception{
        System.out.println("Start process");
        SaveBQTableAsJson sta = new SaveBQTableAsJson(args);
        String executionDate=sta.getToday();
        for (int i=0; i< args.length;i++) {
            if(args[i].equals("--runner")) sta.runner=args[i+1];
            if(args[i].equals("--executionDate")) executionDate=args[i+1];
        }
        long start=System.currentTimeMillis();
        sta.executeQueueGeneral(executionDate);
        System.out.println("End of process "+(System.currentTimeMillis()-start)/1000+" sec");
    }
}



