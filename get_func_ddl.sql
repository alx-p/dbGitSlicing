CREATE OR REPLACE FUNCTION public.get_func_ddl (
  p_schema_name varchar,
  p_function_name varchar
)
RETURNS text AS
$body$
select string_agg(t.col1, chr(10)) func_text 
  from (select pg_get_functiondef(f.oid) col1
          from pg_catalog.pg_proc f 
         inner join pg_catalog.pg_namespace n on f.pronamespace = n.oid
         where f.proname = p_function_name
           and n.nspname = p_schema_name
        union
        select '---------- Описание функции ----------'||chr(10)||'/*'||chr(10) || ds.description||chr(10)||'*/' col1
          from pg_proc p 
          left outer join pg_description ds on ds.objoid = p.oid 
         inner join pg_namespace n on p.pronamespace = n.oid 
         where p.proname = p_function_name
           and n.nspname = p_schema_name) t;
$body$
LANGUAGE 'sql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100;

ALTER FUNCTION public.get_func_ddl (p_schema_name varchar, p_function_name varchar)
  OWNER TO postgres;