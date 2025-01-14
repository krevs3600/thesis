SELECT 
    AVG(Q.final) AS avg_final_price, 
    Q.seller, 
    MAX(Q.max_idx) as idx
FROM (
    SELECT MAX(b.price) AS final, a.seller, MAX(b.idx) as max_idx
    FROM auction a
    JOIN bid b ON a.id = b.auction
    WHERE b.date_time < a.expires
    AND a.expires < NOW()
    GROUP BY a.id, a.seller
) Q
GROUP BY Q.seller;