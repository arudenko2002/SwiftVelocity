package TrackAction;

import com.google.cloud.bigquery.*;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GenerateAlerts {
    String project="umg-dev";
    String runner="DirectRunner";
    String from_mongodb_users_velocity="umg-dev.swift_alerts.from_mongodb_users_velocity";
    String sourceTableApple = "umg-swift.swift_alerts.trending_tracks_apple_countries";
    String sourceTableSpotify = "umg-swift.swift_alerts.trending_tracks_countries";
    String destinationTable = "umg-dev.swift_alerts.velocity_alerts";
    String isrc_first_stream_date = "umg-swift.swift_alerts.isrc_first_stream_date";
    String artist_track_images = "umg-swift.swift_alerts.artist_track_images";
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    String[] arguments=null;
    String executionDate="";

    public GenerateAlerts(String[] args) throws Exception{
        this.arguments=args;
        this.project=getArgument("--project");
        this.runner=getArgument("--runner");
        this.from_mongodb_users_velocity = getArgument("--from_mongodb_users_velocity");
        this.sourceTableApple=getArgument("--sourceTableApple");
        this.sourceTableSpotify=getArgument("--sourceTableSpotify");
        this.destinationTable=getArgument("--destinationTable");
        this.isrc_first_stream_date=getArgument("--isrc_first_stream_date");
        this.artist_track_images=getArgument("--artist_track_images");
        this.executionDate=getArgument("--executionDate");
        if(this.executionDate.contains("Not found"))
            executionDate=getToday();
    }

    private String getArgument(String find) {
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals(find))return arguments[i+1];
        }
        return "Not found";
    }

    public boolean checkTableExists(String dataSet, String tableName) throws Exception{
        //String[] args={"--project"+project,"--runner="+runner};
        //String outputTableKey = getArgument("--playlist_track_action");
        //String[] s = outputTableKey.split("\\.");
        //dataSet=s[1];
        //String outputTable=s[2];
        String exec_sql="SELECT count(1) as count FROM `"+project+"."+dataSet+".__TABLES_SUMMARY__` WHERE table_id = '"+tableName+"'";
        //System.out.println(" project="+project+"  ds="+dataSet+"  tableName="+tableName);
        System.out.println(exec_sql);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(exec_sql)
                        .setUseLegacySql(false)
                        .setPriority(QueryJobConfiguration.Priority.BATCH)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        // Wait for the query to complete.
        queryJob = queryJob.waitFor();
        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        // Get the results.
        QueryResponse response = bigquery.getQueryResults(jobId);
        QueryResult qresult = response.getResult();
        // Print all pages of the results.
        ArrayList<String> output = new ArrayList<String>();
        String result = "";
        while (qresult != null) {
            for (List<FieldValue> row : qresult.iterateAll()) {
                String fn0 = row.get(0).getValue().toString();
                long fv0 = row.get(0).getLongValue();

                System.out.println( "number of records="+fv0+" fn0="+fn0);
                if(fv0==0) {
                    //System.out.println(" table="+project+"."+dataSet+"."+tableName+" does not exist");TODO remove return false
                    return false;
                } else {
                    //System.out.println(" table="+project+"."+dataSet+"."+tableName+" exists");
                    return true;
                }
            }
            qresult = qresult.getNextPage();
        }

        return false;
    }

    void createTable(String dataSet,String tableName) throws Exception{
        TableId tableId = null;
        if (checkTableExists(dataSet,tableName)) {
            System.out.println("Table "+tableName+" already exists, exiting...");
            return;
        }
        if (StringUtils.isNotEmpty(project)) {
            tableId = TableId.of(project, dataSet, tableName);
        } else {
            tableId = TableId.of(dataSet, tableName);
        }

        List<Field> fields = new ArrayList<>();
        fields.add(Field.of("transaction_date", Field.Type.timestamp()));
        fields.add(Field.of("product_id", Field.Type.string()));
        fields.add(Field.of("sales_country_code", Field.Type.string()));
        Schema schema = Schema.of(fields);

        StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder()
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));

        TableDefinition tableDefinition = builder.build();
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = bigquery.create(tableInfo);
        System.out.println("Table created");
    }

    void executeQueue(String query, String dataSet, String tableName,String actualDate) throws Exception{
        //System.out.println(queue);
        System.out.println("ActualDate="+actualDate+"  Project="+project+"  dataSet="+dataSet+"  tableName="+tableName+"$"+actualDate);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query)
                        .setDestinationTable(TableId.of(project,dataSet, tableName+"$"+actualDate))
                        .setUseLegacySql(false)
                        .setFlattenResults(true)
                        .setAllowLargeResults(true)
                        .setPriority(QueryJobConfiguration.Priority.BATCH)
                        //.setUseQueryCache(false)
                        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();
        System.out.println("END OF QUERY");
        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        System.out.println("END OF MAJOR QUERY");
        // Get the results.
        QueryResponse response = bigquery.getQueryResults(jobId);
        QueryResult qresult = response.getResult();
        System.out.println("# of records="+qresult.getTotalRows());
        if(qresult.getTotalRows() == 0)  {
            String result = "Velocity Alerts: No track records found in velocity_alerts for execution date "+actualDate+"!";
            System.out.println("ERROR: "+result+"  Sending email and exiting...");
            sendEmail("ERROR: "+result,"justtome");
        }
    }

    private void sendEmail(String body, String whom) throws Exception{
        SSLEmail ssle = new SSLEmail();
        if(Boolean.parseBoolean(getArgument("--gmail"))) {
            ssle.sendMail("alexey.rudenko@umusic.com", body, body, whom);
        } else {
            ssle.sendUMGMail("alexey.rudenko@umusic.com", "UMG net:" + body, body, whom);
        }
    }

    private String parseSQL(String sql,String actualDate) {
        String result=sql;
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals("--from_mongodb_users_velocity"))result=result.replace("{from_mongodb_users_velocity}",arguments[i+1]);
            if(arguments[i].equals("--sourceTableCountriesApple"))result=result.replace("{source_table_countries_apple}",arguments[i+1]);
            if(arguments[i].equals("--sourceTableCountriesSpotify"))result=result.replace("{source_table_countries_spotify}",arguments[i+1]);
            if(arguments[i].equals("--artist_track_images"))result=result.replace("{artist_track_images}",arguments[i+1]);
            if(arguments[i].equals("--isrc_first_stream_date"))result=result.replace("{isrc_first_stream_date}",arguments[i+1]);
            if(arguments[i].equals("--artist_track_images"))result=result.replace("{artist_track_images}",arguments[i+1]);
        }
        result= result.replace("{actualDate}",actualDate);
        System.out.println(result);
        return result;
    }

    private String getSQL(String actualDate) {
        try {
            InputStream is = GenerateAlerts.class.getClassLoader().getResourceAsStream("insert.sql");
            System.out.println(is);
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            StringBuffer stringBuffer = new StringBuffer();
            String line = null;
            while ((line = reader.readLine()) != null) {
                stringBuffer.append(line).append("\n");
            }
            String sql =  stringBuffer.toString();
            String fullsql = parseSQL(sql,actualDate);
            return fullsql;
        } catch(IOException e) {
            System.out.println("IOException");
            return null;
        }
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

    public void procedure() throws Exception{
        String actualDate = getDaysAgo(executionDate,-1);
        String sql = getSQL(actualDate);
        String[] ss=destinationTable.split("\\.");
        String dataSet=ss[1];
        String tableName=ss[2];
        createTable(dataSet,tableName);
        executeQueue(sql, dataSet, tableName, actualDate.replaceAll("-",""));
    }
    public static void main(String[] args) throws Exception{
        GenerateAlerts ga = new GenerateAlerts(args);
        System.out.println("Start process");
        long start=System.currentTimeMillis();
        ga.procedure();
        System.out.println("End of process "+(System.currentTimeMillis()-start)/1000+" sec");
    }
}
