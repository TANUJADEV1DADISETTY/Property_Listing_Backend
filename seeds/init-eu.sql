-- ============================================================
-- EU Region Database Initialization
-- Seeds the properties table with 1000+ rows (EU region)
-- ============================================================

-- Create properties table
CREATE TABLE IF NOT EXISTS properties (
    id             BIGINT PRIMARY KEY,
    price          DECIMAL(12, 2) NOT NULL,
    bedrooms       INTEGER        NOT NULL,
    bathrooms      INTEGER        NOT NULL,
    region_origin  VARCHAR(2)     NOT NULL,
    version        INTEGER        NOT NULL DEFAULT 1,
    updated_at     TIMESTAMP      NOT NULL DEFAULT NOW()
);

-- Create idempotency_keys table for request deduplication
CREATE TABLE IF NOT EXISTS idempotency_keys (
    request_id   VARCHAR(255) PRIMARY KEY,
    response     JSONB         NOT NULL,
    created_at   TIMESTAMP     NOT NULL DEFAULT NOW()
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_properties_region ON properties(region_origin);
CREATE INDEX IF NOT EXISTS idx_properties_updated_at ON properties(updated_at);

-- ============================================================
-- Seed EU data (rows 1201 - 2400, region_origin = 'eu')
-- ============================================================
INSERT INTO properties (id, price, bedrooms, bathrooms, region_origin) VALUES
(1201, 285000.00, 2, 1, 'eu'),
(1202, 420000.00, 3, 2, 'eu'),
(1203, 575000.00, 4, 3, 'eu'),
(1204, 198000.00, 1, 1, 'eu'),
(1205, 745000.00, 5, 4, 'eu'),
(1206, 350000.00, 3, 2, 'eu'),
(1207, 490000.00, 4, 2, 'eu'),
(1208, 225000.00, 2, 1, 'eu'),
(1209, 620000.00, 4, 3, 'eu'),
(1210, 155000.00, 1, 1, 'eu'),
(1211, 400000.00, 3, 2, 'eu'),
(1212, 515000.00, 4, 3, 'eu'),
(1213, 278000.00, 2, 2, 'eu'),
(1214, 780000.00, 5, 4, 'eu'),
(1215, 215000.00, 2, 1, 'eu'),
(1216, 370000.00, 3, 2, 'eu'),
(1217, 645000.00, 5, 3, 'eu'),
(1218, 168000.00, 1, 1, 'eu'),
(1219, 455000.00, 3, 2, 'eu'),
(1220, 590000.00, 4, 3, 'eu'),
(1221, 245000.00, 2, 1, 'eu'),
(1222, 425000.00, 3, 2, 'eu'),
(1223, 510000.00, 4, 2, 'eu'),
(1224, 182000.00, 1, 1, 'eu'),
(1225, 680000.00, 5, 4, 'eu'),
(1226, 310000.00, 2, 2, 'eu'),
(1227, 445000.00, 3, 2, 'eu'),
(1228, 595000.00, 4, 3, 'eu'),
(1229, 208000.00, 2, 1, 'eu'),
(1230, 765000.00, 5, 4, 'eu'),
(1231, 338000.00, 3, 2, 'eu'),
(1232, 478000.00, 4, 2, 'eu'),
(1233, 196000.00, 1, 1, 'eu'),
(1234, 625000.00, 4, 3, 'eu'),
(1235, 268000.00, 2, 2, 'eu'),
(1236, 390000.00, 3, 2, 'eu'),
(1237, 535000.00, 4, 3, 'eu'),
(1238, 158000.00, 1, 1, 'eu'),
(1239, 710000.00, 5, 4, 'eu'),
(1240, 232000.00, 2, 1, 'eu'),
(1241, 412000.00, 3, 2, 'eu'),
(1242, 548000.00, 4, 3, 'eu'),
(1243, 175000.00, 1, 1, 'eu'),
(1244, 748000.00, 5, 4, 'eu'),
(1245, 318000.00, 2, 2, 'eu'),
(1246, 462000.00, 3, 2, 'eu'),
(1247, 608000.00, 4, 3, 'eu'),
(1248, 220000.00, 2, 1, 'eu'),
(1249, 795000.00, 5, 4, 'eu'),
(1250, 345000.00, 3, 2, 'eu');

-- Generate rows 1251-2400 using a series
INSERT INTO properties (id, price, bedrooms, bathrooms, region_origin)
SELECT
    s.id,
    ROUND((120000 + (s.id * 289 % 700000))::numeric, 2),
    1 + (s.id % 5),
    1 + (s.id % 4),
    'eu'
FROM generate_series(1251, 2400) AS s(id)
ON CONFLICT (id) DO NOTHING;
