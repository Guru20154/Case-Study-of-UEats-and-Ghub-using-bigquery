CREATE TEMP FUNCTION extractAllMenuValues(input STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  const data = JSON.parse(input).data;
  if (!data || !data.menus) return [];
 
  const menus = data.menus;
  const menuValues = [];
  for (const key in menus) {
    if (menus.hasOwnProperty(key)) {
      menuValues.push(JSON.stringify(menus[key]));
    }
  }
  return menuValues;
""";
WITH ubereats_hours AS (
  WITH extracted_menus AS (
    SELECT
      vb_name,
      b_name,
      slug,
      timestamp,
      extractAllMenuValues(TO_JSON_STRING(response)) AS menu_values
    FROM
      `arboreal-vision-339901.take_home_v2.virtual_kitchen_ubereats_hours`
  ),
  expanded_hours AS (
    SELECT
      vb_name,
      b_name,
      slug,
      timestamp,
      menu_value_json,
      day_index,
      CASE
        WHEN day_index = 0 THEN 'MONDAY'
        WHEN day_index = 1 THEN 'TUESDAY'
        WHEN day_index = 2 THEN 'WEDNESDAY'
        WHEN day_index = 3 THEN 'THURSDAY'
        WHEN day_index = 4 THEN 'FRIDAY'
        WHEN day_index = 5 THEN 'SATURDAY'
        WHEN day_index = 6 THEN 'SUNDAY'
      END AS day,
      JSON_EXTRACT_SCALAR(value, '$.startTime') AS open_time,
      JSON_EXTRACT_SCALAR(value, '$.endTime') AS close_time,
      ROW_NUMBER() OVER (PARTITION BY slug, day_index ORDER BY timestamp DESC) AS row_num
    FROM
      extracted_menus,
      UNNEST(menu_values) AS menu_value_json,
      UNNEST(JSON_QUERY_ARRAY(menu_value_json, '$.sections[0].regularHours')) AS value,
      UNNEST(JSON_EXTRACT_ARRAY(value, '$.daysBitArray')) AS day_value WITH OFFSET AS day_index
    WHERE
      JSON_VALUE(day_value) = 'true'
  )
  SELECT
    vb_name,
    b_name,
    slug,
    open_time,
    close_time,
    day
  FROM
    expanded_hours
  WHERE
    row_num = 1
  ORDER BY slug, day_index
),




grubhub_hours AS (
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
  ORDER BY slug, day
)




SELECT
  g.`Grubhub slug`,
  CONCAT(g.open_time,' - ',g.close_time) AS `Virtual Restuarant Business Hours`,
  u.slug AS `Uber Eats slug`,
  CONCAT(u.open_time,' - ',u.close_time) AS `Uber Eats Business Hours`,
  g.day,
  CASE
    WHEN ABS(TIMESTAMP_DIFF(
      TIMESTAMP(CONCAT('1970-01-01 ', g.open_time, ':00.000')),
      TIMESTAMP(CONCAT('1970-01-01 ', u.open_time, ':00.000')),
      MINUTE
      )) + ABS(TIMESTAMP_DIFF(
      TIMESTAMP(CONCAT(CASE WHEN g.close_time = '00:00' THEN '1970-01-02 ' ELSE '1970-01-01 ' END, g.close_time, ':00.000')),  
      TIMESTAMP(CONCAT(CASE WHEN u.close_time = '00:00' THEN '1970-01-02 ' ELSE '1970-01-01 ' END, u.close_time, ':00.000')),  
      MINUTE
      )) < 5 THEN 'In Range'
    WHEN ABS(TIMESTAMP_DIFF(
      TIMESTAMP(CONCAT('1970-01-01 ', g.open_time, ':00.000')),  
      TIMESTAMP(CONCAT('1970-01-01 ', u.open_time, ':00.000')),  
      MINUTE
      )) + ABS(TIMESTAMP_DIFF(
      TIMESTAMP(CONCAT(CASE WHEN g.close_time = '00:00' THEN '1970-01-02 ' ELSE '1970-01-01 ' END, g.close_time, ':00.000')),  
      TIMESTAMP(CONCAT(CASE WHEN u.close_time = '00:00' THEN '1970-01-02 ' ELSE '1970-01-01 ' END, u.close_time, ':00.000')),  
      MINUTE
      )) =5 THEN 'Out of Range with 5 mins difference'
    ELSE 'Out of Range'
  END AS is_out_range
FROM
  ubereats_hours u
JOIN
  grubhub_hours g
ON
  u.day = g.day AND u.b_name = g.b_name AND u.vb_name = g.vb_name
