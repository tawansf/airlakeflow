-- Seed/cleanup for Soda simulation. Safe at init: only runs if schemas/tables exist.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'silver' AND table_name = 'bitcoin') THEN
    DELETE FROM silver.bitcoin WHERE crypto_id IN ('bitcoin_sim_eur', 'bitcoin_sim_neg', 'bitcoin_sim_old');
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'bitcoin_raw') THEN
    DELETE FROM bronze.bitcoin_raw WHERE data_ingestao IS NULL AND payload = '{}'::jsonb;
  END IF;
END $$;

-- INSERT INTO silver.bitcoin (crypto_id, currency, price, updated_at)
-- VALUES
--   ('bitcoin_sim_eur', 'eur', 50000.00000000, NOW()),
--   ('bitcoin_sim_neg', 'usd', -100.00000000, NOW()),
--   ('bitcoin_sim_old', 'usd', 50000.00000000, NOW() - INTERVAL '200 hours');
-- INSERT INTO bronze.bitcoin_raw (data_ingestao, payload)
-- VALUES (NULL, '{}'::jsonb);
