-- Azure SQL Database — the finance-grade SLA reporting mart.
--
-- This is the store finance and BI tools hit: exact numbers, joins across dimensions,
-- penalty computation per lane per day. It is a RELATIONAL copy of the lakehouse gold
-- star, chosen for this consumer because the access pattern is analytical joins over a
-- few million rows with BI concurrency — precisely what a SQL engine with real indexes
-- and a query optimizer is built for, and precisely what Cosmos (a point-read store)
-- would serve slowly and expensively.
--
-- Grain and keys mirror gold exactly, because the M6 control total reconciles this
-- table against the lakehouse and a shape mismatch would make that check meaningless.

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------

IF OBJECT_ID('dbo.dim_lane', 'U') IS NULL
CREATE TABLE dbo.dim_lane (
    lane_id             VARCHAR(16)  NOT NULL PRIMARY KEY,
    origin_hub_id       VARCHAR(16)  NOT NULL,
    dest_hub_id         VARCHAR(16)  NOT NULL,
    lane_promised_hours SMALLINT     NOT NULL
);

IF OBJECT_ID('dbo.dim_hub', 'U') IS NULL
CREATE TABLE dbo.dim_hub (
    hub_id    VARCHAR(16)  NOT NULL PRIMARY KEY,
    hub_name  VARCHAR(128) NOT NULL,
    city      VARCHAR(64)  NOT NULL
);

IF OBJECT_ID('dbo.dim_date', 'U') IS NULL
CREATE TABLE dbo.dim_date (
    date_key    INT      NOT NULL PRIMARY KEY,   -- yyyyMMdd
    [date]      DATE     NOT NULL,
    [year]      SMALLINT NOT NULL,
    [month]     TINYINT  NOT NULL,
    [day]       TINYINT  NOT NULL,
    day_of_week TINYINT  NOT NULL,
    day_name    VARCHAR(10) NOT NULL
);

-- ---------------------------------------------------------------------------
-- Fact — one row per parcel (the finest useful grain; penalties are per-parcel)
-- ---------------------------------------------------------------------------

IF OBJECT_ID('dbo.fct_sla', 'U') IS NULL
CREATE TABLE dbo.fct_sla (
    parcel_id         VARCHAR(20)  NOT NULL PRIMARY KEY,
    lane_id           VARCHAR(16)  NOT NULL,
    origin_hub_id     VARCHAR(16)  NULL,
    dest_hub_id       VARCHAR(16)  NULL,
    seller_id         VARCHAR(16)  NULL,
    pickup_time       DATETIME2(0) NULL,
    delivered_time    DATETIME2(0) NULL,
    delivery_date_key INT          NULL,          -- NULL = in transit (meaningful, not missing)
    promised_hours    SMALLINT     NULL,
    actual_hours      DECIMAL(9,2) NULL,
    is_delivered      BIT          NOT NULL,
    is_breach         BIT          NOT NULL,
    exception_reason  VARCHAR(40)  NULL,
    -- Foreign keys make the star explicit and let the optimizer trust the joins.
    CONSTRAINT fk_fct_lane FOREIGN KEY (lane_id)           REFERENCES dbo.dim_lane(lane_id),
    CONSTRAINT fk_fct_date FOREIGN KEY (delivery_date_key) REFERENCES dbo.dim_date(date_key)
);

-- ---------------------------------------------------------------------------
-- Indexes — shaped to the finance queries, not sprinkled at random
-- ---------------------------------------------------------------------------

-- "Breaches per lane per day" is THE penalty query. A composite on (delivery_date_key,
-- lane_id) filtered by is_breach lets it seek the day, group by lane, and never scan
-- the whole fact. is_breach is INCLUDEd so the index covers the query without a lookup.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_fct_date_lane')
CREATE NONCLUSTERED INDEX ix_fct_date_lane
    ON dbo.fct_sla (delivery_date_key, lane_id)
    INCLUDE (is_breach, is_delivered, actual_hours, promised_hours);

-- "This seller's SLA performance" — a secondary reporting cut.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_fct_seller')
CREATE NONCLUSTERED INDEX ix_fct_seller
    ON dbo.fct_sla (seller_id)
    INCLUDE (is_delivered, is_breach);

-- A filtered index on breaches alone: the penalty report reads only breached parcels,
-- and a filtered index is tiny and hot because it indexes ~a few % of rows.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_fct_breaches_only')
CREATE NONCLUSTERED INDEX ix_fct_breaches_only
    ON dbo.fct_sla (delivery_date_key, lane_id)
    WHERE is_breach = 1;

-- ---------------------------------------------------------------------------
-- The reporting view finance actually queries: lane-daily SLA rollup.
-- Penalties are computed from this — breach rate and count per lane per day.
-- ---------------------------------------------------------------------------

CREATE OR ALTER VIEW dbo.vw_lane_daily_sla AS
SELECT
    f.delivery_date_key,
    d.[date],
    f.lane_id,
    l.origin_hub_id,
    l.dest_hub_id,
    COUNT(*)                                    AS delivered_parcels,
    SUM(CAST(f.is_breach AS INT))               AS breached_parcels,
    CAST(SUM(CAST(f.is_breach AS INT)) AS DECIMAL(9,4))
        / NULLIF(COUNT(*), 0)                   AS breach_rate,
    AVG(f.actual_hours)                         AS avg_actual_hours,
    MAX(l.lane_promised_hours)                  AS promised_hours
FROM dbo.fct_sla f
JOIN dbo.dim_lane l ON l.lane_id = f.lane_id
JOIN dbo.dim_date d ON d.date_key = f.delivery_date_key
WHERE f.is_delivered = 1
GROUP BY f.delivery_date_key, d.[date], f.lane_id, l.origin_hub_id, l.dest_hub_id;
