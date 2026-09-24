CREATE TABLE public.items (id bigint PRIMARY KEY,payload text NOT NULL,version integer NOT NULL DEFAULT 1);
ALTER TABLE public.items REPLICA IDENTITY FULL;
CREATE PUBLICATION flowledger_pub FOR TABLE public.items;
INSERT INTO public.items SELECT i,'initial-'||i,1 FROM generate_series(1,5000) i;
