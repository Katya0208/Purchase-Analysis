-- расширение для UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- таблица клиентов
CREATE TABLE public.clients (
  client_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  first_name        VARCHAR(100) NOT NULL,
  last_name         VARCHAR(100) NOT NULL,
  email             VARCHAR(150) UNIQUE NOT NULL,
  phone             VARCHAR(20),
  address           TEXT,
  registration_date TIMESTAMPTZ NOT NULL DEFAULT now(),
  status            VARCHAR(20) NOT NULL DEFAULT 'active'
);

-- таблица продавцов
CREATE TABLE public.sellers (
  seller_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name          VARCHAR(150) NOT NULL,
  contact_email VARCHAR(150),
  contact_phone VARCHAR(20),
  rating        DECIMAL(3,2) DEFAULT 0.00,
  joined_date   TIMESTAMPTZ NOT NULL DEFAULT now()
);
