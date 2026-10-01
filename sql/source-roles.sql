-- Демо-пароли. Для внешнего стенда создавайте роли с отдельными секретами.
BEGIN;
-- Первоначальный volume содержал только items; подготовка выполняется администратором.
CREATE TABLE IF NOT EXISTS public.stores(id bigint PRIMARY KEY,name text NOT NULL);
ALTER TABLE public.stores REPLICA IDENTITY FULL;
INSERT INTO public.stores VALUES (1,'Первый магазин'),(2,'Второй магазин') ON CONFLICT DO NOTHING;
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='flowledger_owner') THEN
        CREATE ROLE flowledger_owner LOGIN PASSWORD 'owner-demo';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='flowledger_cdc') THEN
        CREATE ROLE flowledger_cdc LOGIN PASSWORD 'cdc-demo';
    END IF;
END $$;
ALTER ROLE flowledger_owner NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE flowledger_cdc NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOBYPASSRLS;
REVOKE ALL ON DATABASE source FROM PUBLIC;
GRANT CONNECT, CREATE ON DATABASE source TO flowledger_owner;
GRANT CONNECT ON DATABASE source TO flowledger_cdc;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO flowledger_owner;
GRANT USAGE ON SCHEMA public TO flowledger_cdc;
ALTER TABLE public.items OWNER TO flowledger_owner;
ALTER TABLE public.stores OWNER TO flowledger_owner;
ALTER PUBLICATION flowledger_pub OWNER TO flowledger_owner;
ALTER PUBLICATION flowledger_pub SET TABLE public.items, public.stores;
REVOKE ALL ON public.items, public.stores FROM PUBLIC, flowledger_cdc;
GRANT SELECT ON public.items, public.stores TO flowledger_cdc;
COMMIT;
