# user_base_project (dbt + BigQuery)

## Datasets (BigQuery)
- Project: `oroboro-dw`
- Sources dataset: `bronze_raw`
- Models dataset: `analytics_dev`

## Build
1) `pip install dbt-bigquery`
2) (optional) `dbt deps` to install packages (`dbt_utils`)
3) `dbt debug`
4) `dbt run --select intermediate.locations_clean intermediate.stacked_users_partners marts.user_base`
5) `dbt test`
6) `dbt docs generate && dbt docs serve`

## Metabase
Connect to dataset `analytics_dev`; query table `user_base` (materialized as table by default).

## Materialization choice: table vs view
- **view**: Always fresh, no storage; runs full SQL at query time (slower dashboards).
- **table**: Stored snapshot at build time; faster dashboards, predictable cost; rebuild on schedule.
- Default here: intermediates = views; `user_base` = table.
