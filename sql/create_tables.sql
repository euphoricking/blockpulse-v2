-- Create crypto_asset_dim table
CREATE TABLE IF NOT EXISTS `blockpulse-insights-project.crypto_data.crypto_asset_dim` (
  `coin_id` STRING NOT NULL,
  `symbol` STRING NOT NULL,
  `name` STRING NOT NULL,
  `image` STRING,
  `market_cap_rank` INT64,
  `genesis_date` DATE,
  `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create date_dim table
CREATE TABLE IF NOT EXISTS `blockpulse-insights-project.crypto_data.date_dim` (
  `date_key` DATE NOT NULL,
  `year` INT64,
  `quarter` INT64,
  `month` INT64,
  `day` INT64,
  `week` INT64,
  `day_of_week` STRING,
  `is_weekend` BOOL,
  `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create crypto_market_snapshot_fact table
CREATE TABLE IF NOT EXISTS `blockpulse-insights-project.crypto_data.crypto_market_snapshot_fact` (
  `coin_id` STRING NOT NULL,
  `symbol` STRING NOT NULL,
  `name` STRING NOT NULL,
  `price` FLOAT64,
  `market_cap` FLOAT64,
  `total_volume` FLOAT64,
  `price_change_percentage_24h` FLOAT64,
  `market_cap_change_percentage_24h` FLOAT64,
  `high_24h` FLOAT64,
  `low_24h` FLOAT64,
  `circulating_supply` FLOAT64,
  `total_supply` FLOAT64,
  `max_supply` FLOAT64,
  `ath` FLOAT64,
  `ath_date` STRING,
  `atl` FLOAT64,
  `atl_date` STRING,
  `snapshot_date` TIMESTAMP,
  `processing_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);