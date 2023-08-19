-- Table: public.syncstats

DROP TABLE IF EXISTS public.syncstats;

CREATE TABLE IF NOT EXISTS public.syncstats
(
    "id" SERIAL PRIMARY KEY,
    "org" character varying(20) COLLATE pg_catalog."default" NOT NULL,
    "date" date NOT NULL,
    "table" character varying(100) COLLATE pg_catalog."default" NOT NULL,
    "nsynced" smallint NOT NULL,
    CONSTRAINT one_stat_per_day EXCLUDE USING gist (
        "org" WITH =,
        "date" WITH =,
        "table" WITH =)

)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.syncstats
    OWNER to analytics;