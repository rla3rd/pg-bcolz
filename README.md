# pgbquery

--DROP FOREIGN TABLE public.testbquery;

CREATE FOREIGN TABLE public.testbquery
   (booleans boolean ,
    date timestamp without time zone ,
    "desc" character varying ,
    floats double precision ,
    letter character varying ,
    "number" integer )
   SERVER multicorn_srv
   OPTIONS (
	primary_key 'letter', 
	rootdir '/home/ralbright/data/test', 
	mode 'a'
);
ALTER FOREIGN TABLE public.testbquery
  OWNER TO quant;

