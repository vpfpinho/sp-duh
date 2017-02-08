CREATE OR REPLACE FUNCTION sharding.trf_updated_sharding_statistics_step()
RETURNS TRIGGER AS $BODY$
BEGIN
  IF TG_OP = 'INSERT' THEN
    NEW.status := 'creating-structure';
    NEW.current_step_at := clock_timestamp();
    NEW.structure_sharding_started_at := NEW.current_step_at;
  ELSE
    IF OLD.status IS NULL OR NEW.status <> OLD.status THEN
      CASE NEW.status
        WHEN 'creating-structure'   THEN NEW.structure_sharding_started_at := clock_timestamp();
        WHEN 'copying-data'         THEN NEW.data_sharding_started_at := clock_timestamp();
        WHEN 'success'              THEN NEW.current_step := NULL;
        ELSE
      END CASE;

      CASE OLD.status
        WHEN 'creating-structure'   THEN NEW.structure_sharding_ended_at := clock_timestamp();
        WHEN 'post-processing-data' THEN NEW.data_sharding_ended_at := clock_timestamp();
        ELSE
      END CASE;
    END IF;
  END IF;

  RETURN NEW;
END;
$BODY$ LANGUAGE plpgsql;