from flowledger.db import bootstrap_source


def main():
    with bootstrap_source() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS public.stores(id bigint PRIMARY KEY,name text NOT NULL)"
        )
        conn.execute("ALTER TABLE public.stores REPLICA IDENTITY FULL")
        conn.execute(
            "INSERT INTO public.stores VALUES (1,'Первый магазин'),(2,'Второй магазин') ON CONFLICT DO NOTHING"
        )


if __name__ == "__main__":
    main()
