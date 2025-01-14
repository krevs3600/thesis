SELECT
    Q.category,
    AVG(Q.final),
    MAX(max_bid_idx) AS idx
FROM (
    SELECT MAX(B.price) AS final, A.category, MAX(B.idx) as max_bid_idx
    FROM auction A, bid B
    WHERE A.id = B.auction AND B.`date_time` BETWEEN A.`date_time` AND A.expires
    GROUP BY A.id, A.category
) Q
GROUP BY Q.category;