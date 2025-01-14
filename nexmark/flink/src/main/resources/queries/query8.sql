SELECT 
    P.id, 
    P.name, 
    A.reserve, 
    P.idx
FROM person P, auction A
WHERE (
    P.id = A.seller 
    AND P.date_time >= NOW() - INTERVAL '12' HOUR 
    AND A.date_time >= NOW() - INTERVAL '12' HOUR
);