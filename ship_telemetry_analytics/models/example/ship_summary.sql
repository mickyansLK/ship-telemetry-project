-- models/ship_metrics.sql

SELECT
  ship_id,
  day,
  avg_speed_knots,
  avg_fuel_level,
  total_records,
  CASE
    WHEN avg_speed_knots > 25 THEN 'High Speed'
    WHEN avg_speed_knots BETWEEN 10 AND 25 THEN 'Cruise'
    ELSE 'Idle'
  END AS speed_category
FROM aggregated_ship_metrics
