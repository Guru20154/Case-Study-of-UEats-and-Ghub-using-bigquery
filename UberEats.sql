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
ORDER BY slug, day_index;