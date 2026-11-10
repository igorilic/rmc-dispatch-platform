-- Orders as the API shows them: once a delivery exists, an order is in progress. Derived when
-- read, so a customer's next snapshot can't reset it.
CREATE VIEW shown_orders AS
SELECT o.*,
       CASE WHEN o.status IN ('unconfirmed', 'pending', 'confirmed')
                 AND EXISTS (SELECT 1 FROM deliveries d WHERE d.order_id = o.id)
            THEN 'in_progress' ELSE o.status END AS shown
FROM orders o;
