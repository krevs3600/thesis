SELECT 
    idx,
    auction, 
    price
FROM bid 
WHERE MOD(auction, 123) = 0;