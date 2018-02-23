"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import date,datetime, timedelta
import os
import getpass


default_args = {
    'owner': 'alexey.rudenko2002@umusic.com',
    'depends_on_past': False,
    #'start_date': datetime(2017, 9, 26),
    #'start_date': datetime.now(),
    #'email': ['airflow@airflow.com'],
    'email': ['arudenko2002@yahoo.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    #'schedule_interval': '30,*,*,*,*',
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

project = '{{var.value.project}}'
runner = '{{var.value.runner}}'
artist_track_images = '{{var.value.artist_track_images}}'
temp_directory = '{{var.value.temp_directory}}'
whom = '{{var.value.whom}}'
gmail = '{{var.value.gmail}}'

from_mongodb_users_velocity = '{{var.value.from_mongodb_users_velocity}}'
velocity_alerts = '{{var.value.velocity_alerts}}'
sourceTableCountriesApple = '{{var.value.sourceTableCountriesApple}}'
sourceTableCountriesSpotify = '{{var.value.sourceTableCountriesSpotify}}'
isrc_first_stream_date = '{{var.value.isrc_first_stream_date_velocity}}'
velocity_topic = '{{var.value.velocity_topic}}'

executionDate=str(datetime.now())[:10]
#executionDate="2018-01-30"
print "executionDate="+executionDate

mongodb = 'java -cp /opt/app/swift-subscriptions/velocity-alerts/SwiftVelocitySubscriptions-0.1.jar TrackAction.MongoDBToBigquery ' \
    + ' --executionDate '+executionDate+ '' \
    + ' --project '+project+ '' \
    + ' --runner '+runner+ '' \
    + ' --temp_directory '+temp_directory+ '' \
    + ' --gmail ' + gmail + '' \
    + ' --from_mongodb_users_velocity ' + from_mongodb_users_velocity+ '' \
    + ' --mongoDB dev'

print mongodb

generateAlerts='java -cp /opt/app/swift-subscriptions/velocity-alerts/SwiftVelocitySubscriptions-0.1.jar TrackAction.GenerateAlerts ' \
    + ' --executionDate '+executionDate \
    + ' --project ' + project \
    + ' --runner ' + runner \
    + ' --temp_directory '+temp_directory+ '' \
    + ' --gmail ' + gmail + '' \
    + ' --from_mongodb_users_velocity ' + from_mongodb_users_velocity \
    + ' --artist_track_images ' + artist_track_images \
    + ' --destinationTable ' + velocity_alerts \
    + ' --sourceTableCountriesApple ' + sourceTableCountriesApple \
    + ' --sourceTableCountriesSpotify ' + sourceTableCountriesSpotify \
    + ' --isrc_first_stream_date ' + isrc_first_stream_date

print generateAlerts

sendMessages = 'java -cp /opt/app/swift-subscriptions/velocity-alerts/SwiftVelocitySubscriptions-0.1.jar TrackAction.SaveBQTableAsJson ' \
    + ' --executionDate '+executionDate \
    + ' --project ' + project \
    + ' --runner ' + runner \
    + ' --temp_directory '+temp_directory \
    + ' --gmail ' + gmail \
    + ' --velocity_alerts ' + velocity_alerts \
    + ' --topic ' + velocity_topic
print sendMessages

dagTask = DAG(
    'velocity_trends_subscriptions', default_args=default_args
    ,start_date=datetime(2018, 01, 29, 0, 0, 0)
    ,schedule_interval='0 17 * * *'
)
dagTask.catchup=False
# t1,t2 and t3 are tasks created by instantiating operators
t11 = BashOperator(
    task_id='build_email_table_mongodb_velocity',
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.TrackActionSubscription --executionDateTest 2017-09-21 --project umg-dev --runner DataflowRunner --mongodb',
    bash_command=mongodb,
    dag=dagTask)

t12 = BashOperator(
    task_id='build_email_table_major_sql_velocity',
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.TrackActionSubscription --executionDateTest 2017-09-21 --project umg-dev --runner DataflowRunner --enrichment',
    bash_command=generateAlerts,
    dag=dagTask)

t13 = BashOperator(
    task_id='pubsub_velocity',
    bash_command=sendMessages,
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.TrackActionSubscription --executionDateTest 2017-09-21 --project umg-dev --runner DataflowRunner --major_sql',
    dag=dagTask)

t11>>t12>>t13