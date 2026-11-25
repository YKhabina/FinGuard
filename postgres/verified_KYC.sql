SELECT
    c.client_id,
    c.kyc_status,
    c.risk_level,
    cd.card_id,
    cd.payment_system,
    cd.card_type,
    cd.issued_at,
    cd.expires_at
FROM core.clients AS c
JOIN core.cards   AS cd
       ON cd.client_id = c.client_id
WHERE c.kyc_status != 'verified'
  AND cd.status     = 'active'
ORDER BY c.client_id, cd.card_id;
