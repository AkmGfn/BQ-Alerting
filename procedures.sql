CREATE EXTERNAL TABLE `analytics.int_alerting_config_kmeans`
OPTIONS(
  sheet_range="sources_kmeans",
  format="GOOGLE_SHEETS",
  uris=["https://docs.google.com/spreadsheets/d/..........."]
);

create or replace view analytics.alerting_config_kmeans
as
(
select *,
       FORMAT(
               """
               CREATE OR REPLACE MODEL analytics.alerting_%s OPTIONS (model_type='kmeans', num_clusters=4,standardize_features = TRUE) AS SELECT %s FROM %s
               where cast(%s as timestamp) > IFNULL((
                             select cast (timestamp_sub(max(%s), interval %d day) as timestamp)
                             from %s), timestamp_sub(CURRENT_TIMESTAMP(), interval %d day))
                   """,
               regexp_replace(NORMALIZE_AND_CASEFOLD(alert), '[^[:alnum:]]', '_'), ml_columns, table, date_column,
               date_column, train_window_days, table, train_window_days)  as model_ddl,
       FORMAT("""
       CREATE OR REPLACE VIEW analytics.alerting_alerts_%t OPTIONS(description=%T) as (
       SELECT * except (%t, %t, INDEX),
       format(%t) as description,
       %t as entity,
       %t as date_column
FROM
    ML.DETECT_ANOMALIES(MODEL `analytics.alerting_%t`,
                        STRUCT (%F AS contamination),
                        (
                            SELECT *
                            FROM `%t`))
)


           """, regexp_replace(NORMALIZE_AND_CASEFOLD(alert), '[^[:alnum:]]', '_'), alert, date_column, entity_column,
              format_spec, entity_column, date_column,
              regexp_replace(NORMALIZE_AND_CASEFOLD(alert), '[^[:alnum:]]', '_'), anomaly_percentage,
              table)                                                      as view_ddl,
       regexp_replace(NORMALIZE_AND_CASEFOLD(alert), '[^[:alnum:]]', '_') as alert_name
FROM analytics.int_alerting_config_kmeans
where alert is not null);


CREATE OR REPLACE PROCEDURE analytics.alerting_create_models ()
BEGIN
    DECLARE
        models ARRAY <string>;
    DECLARE
        c int64;
    SET
        models = (
            SELECT ARRAY_AGG(model_ddl)
            FROM `analytics.alerting_config_kmeans`);
    SET
        c = 0;
    WHILE
        c < ARRAY_LENGTH(models)
        DO
            EXECUTE IMMEDIATE
                models[
                    OFFSET
                        (c)];
            SET
                c = c + 1;
        END WHILE;
END;

CREATE OR REPLACE PROCEDURE analytics.alerting_create_aggregated_view ()
begin
    declare uq string;
    declare uqs array <string>;
    set uqs = (select array_agg(format(
            """select %T as alert_id,%T as alert, date_column,entity, description, is_anomaly from analytics.%t""",
            table_name,
            description,
            table_name))
               from (select JSON_EXTRACT_SCALAR(option_value) as description, table_name
                     from analytics.INFORMATION_SCHEMA.TABLE_OPTIONS
                     where table_name like 'alerting_alerts_%'
                       and option_name = 'description'));
    set uq = (select string_agg (a, "\n UNION ALL\n") from unnest(uqs) as a);
    execute immediate CONCAT("CREATE OR REPLACE VIEW analytics.alerting_all as (",uq,")");
end;

CREATE OR REPLACE PROCEDURE analytics.alerting_create_kmeans_views()
BEGIN
    DECLARE
        views ARRAY <string>;
    DECLARE
        c int64;
    SET
        views = (
            SELECT ARRAY_AGG(view_ddl)
            FROM `analytics.alerting_config_kmeans`);
    SET
        c = 0;
    WHILE
        c < ARRAY_LENGTH(views)
        DO
            EXECUTE IMMEDIATE
                views[
                    OFFSET
                        (c)];
            SET
                c = c + 1;
        END WHILE;
END;

create procedure analytics.alerting_bootstrap() 
begin 
  call analytics.alerting_create_models();
  call analytics.alerting_create_kmeans_views();
  call analytics.alerting_create_aggregated_view();
end;
