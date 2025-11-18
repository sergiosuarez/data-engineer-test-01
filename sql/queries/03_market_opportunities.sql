-- Market Opportunity Analysis: demand vs supply by neighborhood.
WITH demand_supply AS (
    SELECT
        n.neighborhood_name AS neighborhood,
        COUNT(DISTINCT l.listing_id) AS supply_count,
        AVG(f.occupancy_rate) AS avg_occupancy,
        AVG(f.estimated_revenue) AS avg_revenue,
        SUM(f.number_of_reviews) AS total_reviews
    FROM analytics.dim_neighborhood n
    JOIN analytics.dim_listing l
        ON l.neighborhood = n.neighborhood_name
    LEFT JOIN analytics.fact_listing_daily_metrics f
        ON f.listing_id = l.listing_id
    GROUP BY 1
),
scores AS (
    SELECT
        neighborhood,
        supply_count,
        avg_occupancy,
        avg_revenue,
        total_reviews,
        -- demand score weights occupancy and review volume
        ROUND((0.6 * COALESCE(avg_occupancy,0)) + (0.4 * PERCENT_RANK() OVER (ORDER BY total_reviews) ), 4) AS demand_score,
        -- supply score inversely related to number of listings
        ROUND(1 - PERCENT_RANK() OVER (ORDER BY supply_count), 4) AS supply_score
    FROM demand_supply
),
opportunities AS (
    SELECT
        neighborhood,
        supply_count,
        avg_occupancy,
        avg_revenue,
        total_reviews,
        demand_score,
        supply_score,
        ROUND((demand_score + supply_score) / 2, 4) AS opportunity_score
    FROM scores
)
SELECT
    neighborhood,
    demand_score,
    supply_score,
    opportunity_score,
    CASE
        WHEN demand_score >= 0.7 AND supply_score >= 0.6 THEN 'Invest in new supply'
        WHEN demand_score >= 0.6 AND supply_score BETWEEN 0.4 AND 0.6 THEN 'Monitor pricing adjustments'
        ELSE 'Maintain'
    END AS recommended_action
FROM opportunities
ORDER BY opportunity_score DESC
LIMIT 20;
