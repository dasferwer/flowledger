CREATE TABLE public.items (id bigint PRIMARY KEY,payload text NOT NULL,version integer NOT NULL DEFAULT 1);
ALTER TABLE public.items REPLICA IDENTITY FULL;
CREATE PUBLICATION flowledger_pub FOR TABLE public.items;
INSERT INTO public.items SELECT i,'initial-'||i,1 FROM generate_series(1,5000) i;
CREATE TABLE public.stores(id bigint PRIMARY KEY,name text NOT NULL);
ALTER TABLE public.stores REPLICA IDENTITY FULL;
INSERT INTO public.stores VALUES (1,'Первый магазин'),(2,'Второй магазин');
