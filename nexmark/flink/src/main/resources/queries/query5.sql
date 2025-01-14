SELECT 
    auction,
    last_idx AS idx,
    window_start,
    window_end
FROM (
     SELECT 
        auction, 
        COUNT(*) AS num, 
        MAX(idx) AS last_idx,
        window_start,
        window_end
    FROM TABLE (
        HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL '10' SECOND, INTERVAL '20' SECOND)
    )
    GROUP BY auction, window_start, window_end
)