-- Pricing Intelligence: identify over/under-priced listings relative to peers.
WITH listing_metrics AS (
    SELECT
        l.listing_id,
        l.listing_name,
        l.neighborhood,
        l.room_type,
        l.price::numeric(10,2) AS current_price,
        l.accommodates,
        l.bedrooms,
        l.bathrooms,
        l.property_type_key
    FROM analytics.dim_listing l
    WHERE l.is_current = TRUE
),
peer_groups AS (
    SELECT
        listing_id,
        listing_name,
        neighborhood,
        room_type,
        current_price,
        AVG(current_price) OVER (PARTITION BY neighborhood, room_type) AS market_average
    FROM listing_metrics
),
classified AS (
    SELECT
        listing_id,
        listing_name,
        neighborhood,
        room_type,
        current_price,
        market_average,
        CASE
            WHEN market_average = 0 THEN 0
            ELSE ROUND(((current_price - market_average) / market_average) * 100, 2)
        END AS price_difference_pct
    FROM peer_groups
)
SELECT
    listing_id,
    listing_name,
    neighborhood,
    room_type,
    current_price,
    market_average,
    price_difference_pct,
    CASE
        WHEN price_difference_pct >= 15 THEN 'overpriced'
        WHEN price_difference_pct <= -15 THEN 'underpriced'
        ELSE 'fair'
    END AS recommendation
FROM classified
ORDER BY ABS(price_difference_pct) DESC;
