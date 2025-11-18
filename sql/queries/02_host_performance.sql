-- Host Performance Ranking: composite score based on revenue, reviews, and portfolio size.
WITH host_metrics AS (
    SELECT
        h.host_id,
        h.host_name,
        COUNT(DISTINCT l.listing_id) AS active_listings,
        AVG(COALESCE(f.estimated_revenue, 0)) AS avg_estimated_revenue,
        AVG(COALESCE(f.review_scores_rating, 0)) AS avg_rating,
        SUM(COALESCE(f.number_of_reviews, 0)) AS total_reviews
    FROM analytics.dim_host h
    JOIN analytics.dim_listing l
        ON l.host_id = h.host_id
    LEFT JOIN analytics.fact_listing_daily_metrics f
        ON f.listing_id = l.listing_id
    WHERE h.is_current = TRUE
    GROUP BY 1,2
),
scored AS (
    SELECT
        host_id,
        host_name,
        active_listings,
        avg_estimated_revenue,
        avg_rating,
        total_reviews,
        -- normalize components to 0-1 range using percent_ranks
        NTILE(100) OVER (ORDER BY avg_estimated_revenue) / 100.0 AS revenue_score,
        NTILE(100) OVER (ORDER BY avg_rating) / 100.0 AS rating_score,
        NTILE(100) OVER (ORDER BY total_reviews) / 100.0 AS review_volume_score,
        NTILE(100) OVER (ORDER BY active_listings) / 100.0 AS portfolio_score
    FROM host_metrics
),
weighted AS (
    SELECT
        host_id,
        host_name,
        active_listings,
        avg_estimated_revenue,
        avg_rating,
        total_reviews,
        ROUND(
            (0.4 * revenue_score) +
            (0.25 * rating_score) +
            (0.2 * review_volume_score) +
            (0.15 * portfolio_score),
            4
        ) AS performance_score
    FROM scored
)
SELECT
    host_id,
    host_name,
    performance_score,
    ROW_NUMBER() OVER (ORDER BY performance_score DESC) AS ranking,
    JSON_BUILD_OBJECT(
        'avg_estimated_revenue', avg_estimated_revenue,
        'avg_rating', avg_rating,
        'total_reviews', total_reviews,
        'active_listings', active_listings
    ) AS key_metrics_breakdown
FROM weighted
ORDER BY ranking
LIMIT 100;
