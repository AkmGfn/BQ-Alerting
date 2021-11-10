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
END
