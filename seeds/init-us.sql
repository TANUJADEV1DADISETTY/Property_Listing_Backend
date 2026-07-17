-- ============================================================
-- US Region Database Initialization
-- Seeds the properties table with 1000+ rows (US region)
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
-- Seed US data (rows 1 - 1200, region_origin = 'us')
-- ============================================================
INSERT INTO properties (id, price, bedrooms, bathrooms, region_origin) VALUES
(1, 245000.00, 2, 1, 'us'),
(2, 389000.00, 3, 2, 'us'),
(3, 512000.00, 4, 3, 'us'),
(4, 178500.00, 1, 1, 'us'),
(5, 695000.00, 5, 4, 'us'),
(6, 320000.00, 3, 2, 'us'),
(7, 455000.00, 4, 2, 'us'),
(8, 210000.00, 2, 1, 'us'),
(9, 580000.00, 4, 3, 'us'),
(10, 142000.00, 1, 1, 'us'),
(11, 370000.00, 3, 2, 'us'),
(12, 485000.00, 4, 3, 'us'),
(13, 265000.00, 2, 2, 'us'),
(14, 720000.00, 5, 4, 'us'),
(15, 198000.00, 2, 1, 'us'),
(16, 340000.00, 3, 2, 'us'),
(17, 610000.00, 5, 3, 'us'),
(18, 155000.00, 1, 1, 'us'),
(19, 425000.00, 3, 2, 'us'),
(20, 550000.00, 4, 3, 'us'),
(21, 230000.00, 2, 1, 'us'),
(22, 395000.00, 3, 2, 'us'),
(23, 475000.00, 4, 2, 'us'),
(24, 168000.00, 1, 1, 'us'),
(25, 640000.00, 5, 4, 'us'),
(26, 285000.00, 2, 2, 'us'),
(27, 415000.00, 3, 2, 'us'),
(28, 560000.00, 4, 3, 'us'),
(29, 195000.00, 2, 1, 'us'),
(30, 730000.00, 5, 4, 'us'),
(31, 310000.00, 3, 2, 'us'),
(32, 445000.00, 4, 2, 'us'),
(33, 185000.00, 1, 1, 'us'),
(34, 590000.00, 4, 3, 'us'),
(35, 255000.00, 2, 2, 'us'),
(36, 360000.00, 3, 2, 'us'),
(37, 500000.00, 4, 3, 'us'),
(38, 145000.00, 1, 1, 'us'),
(39, 670000.00, 5, 4, 'us'),
(40, 218000.00, 2, 1, 'us'),
(41, 380000.00, 3, 2, 'us'),
(42, 515000.00, 4, 3, 'us'),
(43, 162000.00, 1, 1, 'us'),
(44, 710000.00, 5, 4, 'us'),
(45, 295000.00, 2, 2, 'us'),
(46, 430000.00, 3, 2, 'us'),
(47, 575000.00, 4, 3, 'us'),
(48, 205000.00, 2, 1, 'us'),
(49, 760000.00, 5, 4, 'us'),
(50, 325000.00, 3, 2, 'us');

-- Generate rows 51-1200 using a series
INSERT INTO properties (id, price, bedrooms, bathrooms, region_origin)
SELECT
    s.id,
    ROUND((100000 + (s.id * 317 % 800000))::numeric, 2),
    1 + (s.id % 5),
    1 + (s.id % 4),
    'us'
FROM generate_series(51, 1200) AS s(id)
ON CONFLICT (id) DO NOTHING;
