WITH AuctionCounts AS (
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
),
MaxAuctionCounts AS (
    SELECT 
        MAX(num) AS max_num,
        window_start,
        window_end
    FROM AuctionCounts
    GROUP BY window_start, window_end
)
SELECT 
    a.auction,
    a.last_idx as idx,
    a.num,
    a.window_start,
    a.window_end
FROM AuctionCounts a
JOIN MaxAuctionCounts m
ON a.window_start = m.window_start AND a.window_end = m.window_end
WHERE a.num >= m.max_num;