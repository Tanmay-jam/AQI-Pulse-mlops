-- ─────────────────────────────────────────────────────────────
-- Application schema for the `aqi` database — the single source
-- of truth for all table definitions (no lazy DDL in app code).
--
-- Runs once on first Postgres init, after init-multiple-dbs.sql
-- (mounted into /docker-entrypoint-initdb.d with a later sort key).
-- To re-apply on an existing dev stack: docker compose down -v
-- (drops the data volume), or run this file manually via psql.
--
-- Conventions:
--   * All timestamps are UTC (TIMESTAMPTZ).
--   * `ts_hour` columns are truncated to the hour — the join key
--     that ties readings, weather, AQI and forecasts together.
--   * Pollutant concentrations are stored in µg/m³ (normalized at
--     ingest); CO sub-index math converts to mg/m³ where CPCB needs it.
-- ─────────────────────────────────────────────────────────────

-- 0. City registry — one row per configured city, upserted from
--    config.CITIES by the ingest DAG. The centroid is the representative
--    point weather is pulled at, persisted here so spatial features
--    (e.g. station-to-centroid distance/bearing) read a stable DB fact
--    instead of a constant hardcoded in Python.
CREATE TABLE IF NOT EXISTS cities (
    city          TEXT PRIMARY KEY,
    centroid_lat  DOUBLE PRECISION NOT NULL,
    centroid_lon  DOUBLE PRECISION NOT NULL,
    bbox          TEXT,               -- OpenAQ station-selection box, for provenance
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 1. Station registry — one row per OpenAQ location, deduplicated
--    from readings at ingest time. `city` is the join key to
--    city-level weather.
CREATE TABLE IF NOT EXISTS stations (
    station_id   BIGINT PRIMARY KEY,
    name         TEXT,
    city         TEXT             NOT NULL,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    first_seen   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    last_seen    TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- 2. Raw pollutant readings — long format, one row per
--    (station, parameter, timestamp). The landing zone: kept
--    as-received apart from unit normalization to µg/m³.
--    Natural key makes re-ingesting the same hour a no-op.
CREATE TABLE IF NOT EXISTS aqi_readings (
    station_id     BIGINT           NOT NULL,
    city           TEXT             NOT NULL,
    parameter      TEXT             NOT NULL,
    value          DOUBLE PRECISION NOT NULL,
    unit           TEXT,
    timestamp_utc  TIMESTAMPTZ      NOT NULL,
    ingested_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, parameter, timestamp_utc)
);

-- Serves the features DAG's "last N hours per station" scans.
CREATE INDEX IF NOT EXISTS idx_readings_time ON aqi_readings (timestamp_utc);

-- 3. City-level weather from Open-Meteo — one row per (city, hour).
--    Open-Meteo's model grid (~11 km) is coarser than the spread of
--    stations within a city, so one representative point (the city
--    bbox centroid) serves all stations; join via stations.city.
--    is_forecast=true rows are future covariates for t+1..t+6; they
--    are overwritten by observed rows once the hour has passed.
--
--    Fields are chosen per pollution-dispersion mechanism:
--      mixing volume   boundary_layer_height_m (THE dispersion variable)
--      inversion       temperature_c vs temperature_180m_c (warmer aloft = lid)
--      ventilation     wind speed/direction/gusts
--      stagnation      surface_pressure_hpa
--      wet removal     precipitation_mm
--      aerosol physics relative_humidity / dew_point_c (hygroscopic growth, fog)
--      photochemistry  shortwave_radiation_wm2 / cloud_cover_pct (O3, mixing onset)
--    Derived features (inversion strength = T180-T2, ventilation coefficient
--    = BLH × wind) are computed in the feature builder, not stored.
CREATE TABLE IF NOT EXISTS weather_hourly (
    city                     TEXT             NOT NULL,
    ts_hour                  TIMESTAMPTZ      NOT NULL,
    temperature_c            DOUBLE PRECISION,   -- at 2 m
    temperature_180m_c       DOUBLE PRECISION,   -- inversion probe aloft
    relative_humidity        DOUBLE PRECISION,   -- %
    dew_point_c              DOUBLE PRECISION,
    wind_speed_ms            DOUBLE PRECISION,   -- at 10 m
    wind_direction_deg       DOUBLE PRECISION,
    wind_gusts_ms            DOUBLE PRECISION,
    boundary_layer_height_m  DOUBLE PRECISION,
    surface_pressure_hpa     DOUBLE PRECISION,
    cloud_cover_pct          DOUBLE PRECISION,
    shortwave_radiation_wm2  DOUBLE PRECISION,
    precipitation_mm         DOUBLE PRECISION,
    is_forecast              BOOLEAN          NOT NULL,
    updated_at               TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (city, ts_hour)
);

-- 4. Materialized hourly AQI — one wide, feature-ready row per
--    (station, hour), built by the features DAG from aqi_readings
--    via CPCB rolling averages (24 h for PM/NO2/SO2, 8 h for CO/O3)
--    → per-pollutant sub-indices → AQI = max(sub-indices).
--    This table is simultaneously: training labels, forecast input
--    lags, and the ground truth the monitor grades forecasts against.
--    Sub-indices are stored so any AQI value is auditable.
CREATE TABLE IF NOT EXISTS aqi_hourly (
    station_id          BIGINT           NOT NULL,
    ts_hour             TIMESTAMPTZ      NOT NULL,
    pm25_avg24          DOUBLE PRECISION,
    pm10_avg24          DOUBLE PRECISION,
    no2_avg24           DOUBLE PRECISION,
    so2_avg24           DOUBLE PRECISION,
    co_avg8             DOUBLE PRECISION,   -- mg/m³ (CPCB unit for CO)
    o3_avg8             DOUBLE PRECISION,
    si_pm25             DOUBLE PRECISION,
    si_pm10             DOUBLE PRECISION,
    si_no2              DOUBLE PRECISION,
    si_so2              DOUBLE PRECISION,
    si_co               DOUBLE PRECISION,
    si_o3               DOUBLE PRECISION,
    aqi                 DOUBLE PRECISION NOT NULL,
    dominant_pollutant  TEXT             NOT NULL,
    n_readings          INTEGER          NOT NULL,  -- raw rows behind this hour
    computed_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, ts_hour)
);

CREATE INDEX IF NOT EXISTS idx_aqi_hourly_time ON aqi_hourly (ts_hour);

-- 5. Model forecasts — written hourly by the forecast DAG, graded
--    by the monitor DAG joining target_time against aqi_hourly.ts_hour
--    once the target hour's actuals have materialized. model_* columns
--    let champion/challenger errors be compared retroactively.
CREATE TABLE IF NOT EXISTS forecasts (
    station_id     BIGINT           NOT NULL,
    issued_at      TIMESTAMPTZ      NOT NULL,  -- when the forecast was made
    horizon        SMALLINT         NOT NULL CHECK (horizon BETWEEN 1 AND 6),
    target_time    TIMESTAMPTZ      NOT NULL,  -- issued_at + horizon hours
    predicted_aqi  DOUBLE PRECISION NOT NULL,
    lower_ci       DOUBLE PRECISION,
    upper_ci       DOUBLE PRECISION,
    model_name     TEXT             NOT NULL,
    model_version  TEXT             NOT NULL,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, issued_at, horizon)
);

CREATE INDEX IF NOT EXISTS idx_forecasts_target ON forecasts (target_time);
