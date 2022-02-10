CREATE OR REPLACE FUNCTION public.get_table_ddl (
  p_schema_name varchar,
  p_table_name varchar
)
RETURNS text AS
$body$
declare
  l_table_name varchar;

  lreloid oid;
  lrelhasoids bool;
  lrelhasindex bool;
  lrelnatts int2;
  lrelchecks int2;
  lrelreplident varchar;
  lrelhastriggers bool;

  rez text;
  att text;
  con text;
  rec record;
  lconkey text;
  lconkeym smallint[];
  lconkeyf text;
  lf1 text;
  i integer;
  j integer;
  ldescr text;
  pclmn text;
  pclmnnotype text;
  pdata text;
  pmaxlen integer;
  psort text;
begin	
  l_table_name := p_schema_name||'.'||p_table_name;
  
  rez:=''; att:=''; pdata:=''; pclmn:=''; pclmnnotype:=''; psort:='';

  select cl.oid,
         cl.relhasoids, cl.relhasindex, cl.relnatts, cl.relchecks, cl.relreplident, cl.relhastriggers 
    into lreloid,
         lrelhasoids, lrelhasindex, lrelnatts, lrelchecks, lrelreplident, lrelhastriggers
    from pg_class cl
   inner join pg_namespace ns on ns.oid = cl.relnamespace
   where cl.relkind = 'r'
     and ns.nspname = p_schema_name
     and cl.relname = p_table_name;

  if not found then
    return 'Table '||l_table_name||' not found.'::text;
  end if; 

  --SEQUENCE
  select upper(replace(replace(replace(replace(replace(adef.adsrc,'(',''),'nextval''',''), '::regclass)',''),'''::text)',''),'''','')) as sequencename
    into ldescr
    from pg_attribute att
   inner join pg_attrdef adef on adef.adrelid = att.attrelid 
                             and adef.adnum = att.attnum
   inner join pg_class cl on cl.oid = att.attrelid
   inner join pg_namespace ns on ns.oid = cl.relnamespace
   where ns.nspname = p_schema_name 
     and cl.relname = p_table_name 
     and adef.adsrc like '%nextval%';

  if ldescr is not null then 
    if position('.' in ldescr)<>0 then 
      ldescr := substring(ldescr, position('.' in ldescr)+1, length(ldescr));
    end if;
    rez := rez||'CREATE SEQUENCE '||p_schema_name||'.'||ldescr||';'||chr(10)||chr(10);
    ldescr := '';
  end if;

  --TABLE
  rez := rez||'CREATE TABLE '||l_table_name||'('||chr(10);
  for rec in select concat(concat('  ,',att.attname),' '
                   ,case tp.typname 
                      when 'int2' then 'smallint' 
                      when 'int4' then 'integer' 
                      when 'int8' then 'bigint' 
                      when 'varchar' then 'character varying' 
                      when 'timestamp' then 'timestamp without time zone' 
                      when 'timestamptz' then 'timestamp with time zone' 
                      else tp.typname 
                    end,' '
                   ,case att.atttypmod 
                      when -1 then '' 
                      else concat('(',(att.atttypmod-4),')') 
                    end
                   ,case att.attnotnull 
                      when false then '' 
                      else 'NOT NULL' 
                    end,' '
                   ,case att.atthasdef 
                      when false then '' 
                      else concat('DEFAULT ',adef.adsrc) 
                    end) as rezatt
               from pg_attribute att
              inner join pg_type tp on tp.oid = att.atttypid
               left join pg_attrdef adef on adef.adrelid = att.attrelid 
                                        and adef.adnum = att.attnum
              where att.attrelid = lreloid
                and att.attnum > 0
              order by att.attnum
  loop
    att := att||rec.rezatt||chr(10);
  end loop;
  att := trim(leading '  ,' from att);
  rez := rez||'   '||att;

  drop table if exists tempp;
  create TEMPORARY TABLE tempp(conkey integer, conkeyf integer);

  --CONSTRAINT PRIMARY KEY
  i := 1;
  if lrelreplident = 'd' then
    rez := rez||'  ,';
    select concat('CONSTRAINT ', cnst.conname, ' PRIMARY KEY') as f1, cnst.conkey 
      into lf1, lconkeym
      from pg_constraint cnst 
     where cnst.conrelid = lreloid 
       and cnst.contype = 'p';

    while lconkeym[i] is not null loop
      insert into tempp(conkey) (select lconkeym[i]);										  
      i:=i+1; 
    end loop;

    rez := rez||lf1;

    select concat(' (',string_agg(att.attname,','),')') 
      into lconkey
      from pg_attribute att 
     where att.attrelid = lreloid
       and att.attnum in (select conkey from tempp);

    rez := rez||lconkey||chr(10);
  end if;

  delete from tempp;		
  lconkey:=''; 
  for j in 1..i 
  loop 
    lconkeym[j] := null; 
  end loop;
  i:=1; j:=1; lf1 := null;
    
  --CONSTRAINT UNIQUE
  if exists (select 1 
               from pg_constraint cnst 
              where cnst.conrelid = lreloid 
                and cnst.contype = 'u') then
    rez:=rez||'  ,';
    for rec in select concat('  ,CONSTRAINT ', cnst.conname, ' UNIQUE') as f1, 
                         cnst.conkey
                    from pg_constraint cnst 
                    left join pg_class cl on cl.oid = cnst.confrelid
                   where cnst.conrelid = lreloid
                     and cnst.contype ='u'
    loop
      while rec.conkey[i] is not null loop
        insert into tempp(conkey,conkeyf) (select rec.conkey[i],0);										  
        i:=i+1; 
      end loop;

      select concat(' (',string_agg(att.attname,','),') ',ldescr) 
        into lconkey
        from pg_attribute att 
       where att.attrelid = lreloid 
         and att.attnum in (select t.conkey from tempp t where t.conkeyf=0);

      rez:=rez||rec.f1||lconkey||chr(10);
      i:=1;
      delete from tempp;
    end loop;
  end if;  

  delete from tempp;
  lconkey:=''; rez:=replace(rez, ',  ,CONSTRAINT',',CONSTRAINT');

  --CONSTRAINT FOREIGN KEY
  if exists (select 1 
               from pg_constraint cnst 
              where cnst.conrelid = lreloid 
                and cnst.contype = 'f') then
    rez:=rez||'  ,';

    for rec in select concat('  ,CONSTRAINT ',cnst.conname, ' FOREIGN KEY') as f1
                         ,case cnst.confrelid 
                            when 0 then '' 
                            else concat('     REFERENCES ',p_schema_name,'.',cl.relname) 
                          end as f2
                         ,concat((case cnst.confmatchtype 
                                    when 'f' then ' MATCH FULL' 
                                    when 'p' then 'MATCH PARTIAL' 
                                    when 's' then 'MATCH SIMPLE' 
                                    else '' 
                                  end)
                         ,chr(10),(case cnst.confupdtype when 'a' then '     ON UPDATE NO ACTION' 
                                                 when 'r' then '     ON UPDATE RESTRICT' 
                                                 when 'c' then '     ON UPDATE CASCADE' 
                                                 when 'n' then '     ON UPDATE SET NULL' 
                                                 when 'd' then '     ON UPDATE SET DEFAULT' 
                                                 else '' end)
                         ,chr(10),(case cnst.confdeltype when 'a' then '     ON DELETE NO ACTION' 
                                                 when 'r' then '     ON DELETE RESTRICT' 
                                                 when 'c' then '     ON DELETE CASCADE' 
                                                 when 'n' then '     ON DELETE SET NULL' 
                                                 when 'd' then '     ON DELETE SET DEFAULT' 
                                                 else '' end)) as f3
                         ,cnst.conkey, cnst.confkey, cl.oid
                    from pg_constraint cnst 
                    left join pg_class cl on cl.oid = cnst.confrelid
                   where cnst.conrelid = lreloid
                     and cnst.contype = 'f'
    loop
      while rec.conkey[i] is not null loop
        insert into tempp(conkey,conkeyf) (select rec.conkey[i],0);										  
        i:=i+1; 
      end loop;

      while rec.confkey[j] is not null loop
        insert into tempp(conkey,conkeyf) (select rec.confkey[j],1);										  
        j:=j+1; 
      end loop;

      select concat(' (',string_agg(att.attname,','),') ',ldescr) 
        into lconkey
        from pg_attribute att 
       where att.attrelid = lreloid 
         and att.attnum in (select t.conkey from tempp t where t.conkeyf=0);

      select concat(' (',string_agg(att.attname,','),') ',ldescr) 
        into lconkeyf
        from pg_attribute att 
       where att.attrelid = rec.oid 
         and att.attnum in (select t.conkey from tempp t  where t.conkeyf=1);

      rez:=rez||rec.f1||lconkey||chr(10)||rec.f2||lconkeyf||rec.f3||chr(10);
      i:=1;
      j:=1;
      delete from tempp;
    end loop;
  end if;  

  delete from tempp;
  lconkey:=''; lconkeyf:=''; rez:=replace(rez, ',  ,CONSTRAINT',',CONSTRAINT');
  				   
  --CONSTRAINT CHECK
  if lrelchecks then
    rez:=rez||'  ,';

    select concat('CONSTRAINT ',cnst.conname,' CHECK') as f1, cnst.conkey, cnst.consrc
      into lf1, lconkeym, ldescr
      from pg_constraint cnst 
     where cnst.conrelid = lreloid 
       and cnst.contype = 'c';

    while lconkeym[i] is not null loop
      insert into tempp(conkey) (select lconkeym[i]);										  
      i:=i+1; 
    end loop;

    rez:=rez||lf1;

    select concat(' (',string_agg(att.attname,','),') ',ldescr) 
      into lconkey
      from pg_attribute att 
     where att.attrelid = lreloid 
       and att.attnum in (select conkey from tempp);

    rez:=rez||lconkey||chr(10);
  end if;

  delete from tempp;

  lconkey:=''; for j in 1..i loop lconkeym[j]:=null; end loop;i:=1;j:=1;lf1:=null;ldescr:=null;		   

  if lrelhasoids then 
    rez:=rez||') WITH OIDS);'||chr(10); 
  else 
    rez:=rez||') WITH (OIDS = FALSE);'||chr(10)||chr(10); 
  end if;

  --COMMENTS
  if exists (select 1 
               from pg_description des 
              where des.objoid = lreloid) then

    for rec in select des.objsubid, des.description 
                     ,case des.objsubid when 0 
                        then 'COMMENT ON TABLE '||p_schema_name||'.'||p_table_name||chr(10)||' IS '''||des.description||''';' 
                        else 'COMMENT ON COLUMN '||p_schema_name||'.'||p_table_name||'.'||att.attname||chr(10)||' IS '''||des.description||''';' 
                      end as f1
                 from pg_description des
                 left join pg_attribute att on att.attnum = des.objsubid 
                                           and att.attrelid = lreloid
                where des.objoid = lreloid
                order by 1
    loop
      rez:=rez||rec.f1||chr(10)||chr(10);		   
    end loop;
  end if;

  --INDEXES
  if lrelhasindex then
    for rec in select cl.relname,idx.indnatts,am.amname
                    from pg_index idx
                   inner join pg_class cl on cl.oid = idx.indexrelid
                   inner join pg_am am on am.oid = cl.relam
                   where idx.indrelid = lreloid
                     and idx.indisprimary = false 
                   order by 1
    loop
      lconkey:=lconkey||'CREATE INDEX '||rec.relname||chr(10)||'  ON '||p_schema_name||'.'||p_table_name||chr(10)||'  USING '||upper(rec.amname)||' ('||rec.indnatts||' key) '||'TABLESPACE pg_default;'||chr(10);
    end loop;
    rez:=rez||lconkey;lconkey:='';
  end if;
    
  rez:=rez||chr(10);

  --TRIGGERS
  if lrelhastriggers then
    for rec in select tr.tgname,tr.tgtype,tr.tgnargs
                        ,case tr.tgtype 
                           when 5 then ' AFTER INSERT ' 
                           when 7 then ' BEFORE INSERT '
                           when 9 then ' AFTER DELETE ' 
                           when 11 then ' BEFORE DELETE ' 
                           when 17 then ' AFTER UPDATE ' 
                           when 19 then ' BEFORE UPDATE '
                           else '' 
                         end as f1
                        ,pr.proname
                        ,case tr.tgnargs when 0 then '' else tr.tgnargs::varchar end as f2
                    from pg_trigger tr
                   inner join pg_proc pr on pr.oid = tr.tgfoid
                   where tr.tgisinternal = false 
                     and tr.tgrelid = lreloid
    loop
      lconkey:=lconkey||'CREATE TRIGGER '||rec.tgname||chr(10)||rec.f1||chr(10)||' ON '||p_schema_name||'.'||p_table_name||chr(10)||
               ' FOR EACH ROW '||chr(10)||'EXECUTE PROCEDURE '||p_schema_name||'.'||rec.proname||'('||rec.f2||');'||chr(10);
    end loop;
    rez:=rez||lconkey;lconkey:='';
  end if;

  --DATA
  if exists (select 1 
               from common.spr_reposit 
              where nspname = p_schema_name 
                and relname = p_table_name) then
      
    drop table if exists tmpdat;
    create temporary table tmpdat(valchar varchar);  

    for rec in select att.attname, att.attnum
                 from pg_attribute att
                where att.attrelid = lreloid
                  and att.attnum > 0
                order by att.attnum
    loop
      if rec.attnum=1 then psort:=rec.attname; end if;
      pclmn:=pclmn||', '||rec.attname;
      execute 'select max(length(coalesce(cast('||rec.attname||' as varchar),''null''))) as integer from '||p_schema_name||'.'||p_table_name into pmaxlen;
      pclmnnotype:=pclmnnotype||'rpad((coalesce(cast('||rec.attname||' as varchar),''null'')),'||pmaxlen+2||')||'',''||';
    end loop;

    pclmn:=trim(leading ', ' from pclmn);
    pclmnnotype:=trim(trailing '||'',''|| ' from pclmnnotype);
    pdata:='insert into '||p_schema_name||'.'||p_table_name||'('||pclmn||') values '||chr(10);
    execute 'insert into tmpdat (valchar) (select '||pclmnnotype||' from '||p_schema_name||'.'||p_table_name||' order by '||psort||')';
    for rec in select valchar from tmpdat
    loop
      pdata:=pdata||'( '||rec.valchar||'),'||chr(10);
    end loop;
    rez:=rez||pdata;
    pdata:='';
  end if;

  --raise info 'rez-%',rez;

  return rez;
end;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
PARALLEL UNSAFE
COST 100;

ALTER FUNCTION public.get_table_ddl (p_schema_name varchar, p_table_name varchar)
  OWNER TO postgres;