SELECT person.name, person.city, person.state, auction.id, person.idx
FROM auction, person
WHERE 
    auction.seller = person.id
    AND (person.state = 'or' OR person.state = 'id' OR person.state = 'ca')
    AND auction.category = 10;