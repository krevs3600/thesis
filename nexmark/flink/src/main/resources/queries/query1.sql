SELECT
    idx,
    auction,
    bidder,
    0.908 * price as price, -- convert dollar to euro
    channel,
    url,
    date_time,
    extra
FROM bid;