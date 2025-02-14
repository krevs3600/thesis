SELECT person.name, person.city, person.state, auction.id, person.idx
FROM auction
JOIN person
ON auction.seller = person.id
WHERE 
    (person.state = 'or' OR person.state = 'id' OR person.state = 'ca')
    AND auction.category = 10;