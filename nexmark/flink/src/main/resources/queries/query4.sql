SELECT
    Q.category_id,
    AVG(Q.final_price),
    MAX(max_bid_idx) AS idx 
FROM (
    SELECT 
        A.id as auction_id, 
        A.category as category_id, 
        MAX(B.price) AS final_price, 
        MAX(B.idx) as max_bid_idx -- can'd directly do this but in flink there is no other way I guess, but is likely probable that new bids have larger price
    FROM auction A, bid B
    WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires
    GROUP BY A.id, A.category
) Q
GROUP BY Q.category_id;