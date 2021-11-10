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

CREATE OR REPLACE PROCEDURE alerting.create_aggregated_view ()
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
end
