WITH latest_grubhub_hours AS (
  SELECT
    vb_name,
    b_name,
    slug,
    timestamp,
    value,
    JSON_EXTRACT_SCALAR(day_value, '$') AS day_of_week,
    ROW_NUMBER() OVER (PARTITION BY slug, JSON_EXTRACT_SCALAR(day_value, '$') ORDER BY timestamp DESC) AS row_num
  FROM
    `arboreal-vision-339901.take_home_v2.virtual_kitchen_grubhub_hours`,
    UNNEST(JSON_QUERY_ARRAY(response, '$.availability_by_catalog.STANDARD_DELIVERY.schedule_rules')) AS value,
    UNNEST(JSON_EXTRACT_ARRAY(value, '$.days_of_week')) AS day_value
)
SELECT
  vb_name,
  b_name,
  slug AS `Grubhub slug`,
  FORMAT_TIME('%H:%M', CAST(CONCAT(JSON_EXTRACT_SCALAR(value, '$.from')) AS TIME)) AS open_time,
  FORMAT_TIME('%H:%M', CAST(CONCAT(JSON_EXTRACT_SCALAR(value, '$.to')) AS TIME)) AS close_time,
  day_of_week AS day
FROM
  latest_grubhub_hours
WHERE row_num = 1
ORDER BY slug, day;
