SELECT 
    B.auction, 
    B.price, 
    B.bidder, 
    max_idx as idx, 
    window_start, 
    window_end
FROM (
    SELECT auction, price, bidder, MAX(price) AS max_price, MAX(idx) AS max_idx, window_start, window_end 
    FROM TABLE (
        HOP(
            TABLE bid,              
            DESCRIPTOR(date_time),
            INTERVAL '10' SECOND,  
            INTERVAL '20' SECOND  
        )
    )
    GROUP BY auction, price, bidder, window_start, window_end
) B
WHERE B.price = B.max_price;