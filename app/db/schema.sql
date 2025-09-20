CREATE TABLE IF NOT EXISTS brands(
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  domain TEXT NOT NULL,
  category TEXT NOT NULL,
  facebook_page_id TEXT
);

CREATE TABLE IF NOT EXISTS products(
  id BIGSERIAL PRIMARY KEY,
  brand_id INT REFERENCES brands(id),
  handle TEXT,
  title TEXT,
  url TEXT
);

CREATE TABLE IF NOT EXISTS variants(
  id BIGSERIAL PRIMARY KEY,
  product_id BIGINT REFERENCES products(id),
  sku TEXT,
  options JSONB
);

CREATE TABLE IF NOT EXISTS prices(
  variant_id BIGINT REFERENCES variants(id),
  ts_date DATE NOT NULL,
  currency TEXT NOT NULL,
  price_cents INT,
  compare_at_cents INT,
  available BOOLEAN,
  PRIMARY KEY (variant_id, ts_date)
);

CREATE TABLE IF NOT EXISTS ads_daily(
  brand_id INT REFERENCES brands(id),
  ts_date DATE NOT NULL,
  active_ads INT,
  new_ads_24h INT,
  PRIMARY KEY (brand_id, ts_date)
);

CREATE TABLE IF NOT EXISTS leaders(
  ts_date DATE NOT NULL,
  brand_id INT REFERENCES brands(id),
  score NUMERIC,
  rank INT,
  PRIMARY KEY (ts_date, brand_id)
);

CREATE TABLE IF NOT EXISTS users(
  id BIGSERIAL PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  tier TEXT NOT NULL CHECK (tier IN ('free','daily','pro')),
  verified BOOLEAN DEFAULT FALSE,
  unsubscribed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS sends(
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMP NOT NULL,
  kind TEXT CHECK (kind IN ('daily','weekly')),
  user_id BIGINT REFERENCES users(id),
  status TEXT
);
