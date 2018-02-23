WITH
getTrackImages as (
select isrc,max(track_image) as track_image from `{artist_track_images}`
group by 1
),
getLabel as (
  select * from `umg-swift.metadata.local_product`
),
getAvgVelocityApple AS (
  SELECT country_code,AVG(rank_adj_score) as avg_velocity
  FROM `{source_table_countries_apple}`
  where _PARTITIONTIME="{actualDate}"
  and rank_adj_score>0.0
  group by 1
),
getApple as (
-- PARTNER="Apple"
SELECT a.*,b.*,avg_velocity,e.track_image,d.first_stream_date
FROM `{from_mongodb_users_velocity}` a
join  `{source_table_countries_apple}` b
on a.artist=b.canopus_id
join getAvgVelocityApple c
on b.country_code=c.country_code
join `{isrc_first_stream_date}` d
ON b.isrc=d.isrc
LEFT JOIN getTrackImages e
ON b.isrc=e.isrc
where _PARTITIONTIME="{actualDate}"
and b.rank_adj_score>=c.avg_velocity
and partner="Apple"
and first_stream_date>=DATE_SUB(DATE("{actualDate}"), INTERVAL 2 YEAR)
and first_stream_date<DATE_SUB(DATE("{actualDate}"), INTERVAL 2 WEEK)
    and (primaryTerritoryCode=b.country_code
    or  (
            secondaryTerritoryCode is not null
            and secondaryTerritoryCode=b.country_code
        )
    )
),
getAppleLabel as (
-- PARTNER="Apple Labels"
SELECT a.*,b.*,avg_velocity,e.track_image,d.first_stream_date
FROM `{from_mongodb_users_velocity}` a
join getLabel aa
on a.label=aa.sap_segment_code
join  `{source_table_countries_apple}` b
on aa.isrc=b.isrc
join getAvgVelocityApple c
on b.country_code=c.country_code
join `{isrc_first_stream_date}` d
ON b.isrc=d.isrc
LEFT JOIN getTrackImages e
ON b.isrc=e.isrc
where _PARTITIONTIME="{actualDate}"
and b.rank_adj_score>=c.avg_velocity
and partner="Apple"
and first_stream_date>=DATE_SUB(DATE("{actualDate}"), INTERVAL 2 YEAR)
and first_stream_date<DATE_SUB(DATE("{actualDate}"), INTERVAL 2 WEEK)
    and (primaryTerritoryCode=b.country_code
    or  (
            secondaryTerritoryCode is not null
            and secondaryTerritoryCode=b.country_code
        )
    )
),
getAvgVelocity AS (
  SELECT country_code,AVG(rank_adj_score) as avg_velocity
  FROM `{source_table_countries_spotify}`
  where _PARTITIONTIME="{actualDate}"
  and rank_adj_score>0.0
  group by 1
),
getSpotify as (
-- PARTNER="Spotify"
SELECT a.*,b.*,avg_velocity,e.track_image,d.first_stream_date
FROM `{from_mongodb_users_velocity}` a
join  `{source_table_countries_spotify}` b
on a.artist=b.canopus_id
join getAvgVelocity c
on b.country_code=c.country_code
join `{isrc_first_stream_date}` d
ON b.isrc=d.isrc
LEFT JOIN getTrackImages e
ON b.isrc=e.isrc
where _PARTITIONTIME="{actualDate}"
and b.rank_adj_score>=c.avg_velocity
and partner="Spotify"
and first_stream_date>=DATE_SUB(DATE("{actualDate}"), INTERVAL 2 YEAR)
and first_stream_date<DATE_SUB(DATE("{actualDate}"), INTERVAL 2 WEEK)
    and (primaryTerritoryCode=b.country_code
    or  (
            secondaryTerritoryCode is not null
            and secondaryTerritoryCode=b.country_code
        )
    )
),
getSpotifyLabel as (
-- PARTNER="Spotify Labels"
SELECT a.*,b.*,avg_velocity,e.track_image,d.first_stream_date
FROM `{from_mongodb_users_velocity}` a
join getLabel aa
on a.label=aa.sap_segment_code
join  `{source_table_countries_spotify}` b
on aa.isrc=b.isrc
join getAvgVelocity c
on b.country_code=c.country_code
left join `{isrc_first_stream_date}` d
ON b.isrc=d.isrc
LEFT JOIN getTrackImages e
ON b.isrc=e.isrc
where _PARTITIONTIME="{actualDate}"
and b.rank_adj_score>=c.avg_velocity
and partner="Spotify"
and first_stream_date>=DATE_SUB(DATE("{actualDate}"), INTERVAL 2 YEAR)
and first_stream_date<DATE_SUB(DATE("{actualDate}"), INTERVAL 2 WEEK)
    and (primaryTerritoryCode=b.country_code
    or  (
            secondaryTerritoryCode is not null
            and secondaryTerritoryCode=b.country_code
        )
    )
),
getVelocityAlerts as (
select * from getSpotify
UNION all
select * from getSpotifyLabel
UNION ALL
select * from getApple
UNION all
select * from getAppleLabel
)
select * from getVelocityAlerts
