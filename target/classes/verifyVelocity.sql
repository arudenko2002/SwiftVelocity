WITH velocity_base AS (
SELECT "spotify base" as name,_PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.velocity_base_table_v2`
--`umg-swift.swift_alerts.trending_tracks_countries`
WHERE _PARTITIONTIME = "2018-02-03 00:00:00"
group by 1,2
order by pt
),
velocity_countries AS (
SELECT "spotify countries" as name,_PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_countries`
WHERE _PARTITIONTIME = "2018-02-03 00:00:00"
group by 1,2
order by pt
),
velocity_regions AS (
SELECT "spotify regions" as name,_PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_regions`
WHERE _PARTITIONTIME = "2018-02-03 00:00:00"
group by 1,2
order by pt
),
velocity_base_apple AS (
SELECT "apple base" as name,_PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.velocity_base_table_v2_apple`
--`umg-swift.swift_alerts.trending_tracks_apple_regions`
WHERE _PARTITIONTIME = "2018-02-03 00:00:00"
group by 1,2
order by pt
),
velocity_countries_apple AS (
SELECT "apple coutries" as name,_PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_apple_countries`
WHERE _PARTITIONTIME = "2018-02-03 00:00:00"
group by 1,2
order by pt
),
velocity_regions_apple AS (
SELECT "apple regions" as name,_PARTITIONTIME as pt,count(*)  FROM `umg-swift.swift_alerts.trending_tracks_apple_regions`
WHERE _PARTITIONTIME = "2018-02-03 00:00:00"
group by 1,2
order by pt
),
getVelocityAlerts AS (
select "velocity alerts" as name,_PARTITIONTIME as pt,count(*) from `umg-dev.swift_alerts.velocity_alerts`
WHERE _PARTITIONTIME = "2018-02-03 00:00:00"
group by 1,2
order by pt
),
getVelocityEmails AS (
select "velocity emails" as name,_PARTITIONTIME as pt,count(distinct(email)) from `umg-dev.swift_alerts.velocity_alerts`
WHERE _PARTITIONTIME = "2018-02-03 00:00:00"
group by 1,2
order by pt
),
getResult as (

SELECT * FROM velocity_base_apple
UNION ALL
SELECT * FROM velocity_countries_apple
UNION ALL
SELECT * FROM velocity_regions_apple
UNION ALL
SELECT * FROM velocity_base
UNION ALL
SELECT * FROM velocity_regions
UNION ALL
SELECT * FROM velocity_countries
UNION ALL
SELECT * FROM getVelocityAlerts
UNION ALL
SELECT * FROM getVelocityEmails
)

select * from getResult
order by name,pt