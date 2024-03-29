﻿CREATE OR REPLACE FUNCTION public.get_table_ddl (
  p_schema_name varchar,
  p_table_name varchar
)
RETURNS text AS
$body$
declare
  l_table_name varchar;
  l_table_id oid;
  l_conkey_arr smallint[];

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
--  lconkeym smallint[];
  lconkeyf text;
  lf1 text;
--  i integer;
--  j integer;
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
    into l_table_id,
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
  rez := rez||'CREATE TABLE '||l_table_name||' ('||chr(10);
  --  COLUMNS
  select string_agg(concat_ws(' ', '  ,'||att.attname,
                                   case tp.typname 
                                     when 'int2' then 'smallint' 
                                     when 'int4' then 'integer' 
                                     when 'int8' then 'bigint' 
                                     when 'timestamp' then 'timestamp without time zone' 
                                     when 'timestamptz' then 'timestamp with time zone' 
                                     else tp.typname 
                                   end ||
                                     case att.atttypmod 
                                       when -1 then '' 
                                       else concat('(',(att.atttypmod-4),')') 
                                     end,
                                   case when att.attnotnull 
                                     then 'NOT NULL'
                                     else null 
                                   end,
                                   case when att.atthasdef 
                                     then concat('DEFAULT ', adef.adsrc)
                                     else null 
                                   end), chr(10) order by att.attnum) as rezatt
               into att                    
               from pg_attribute att
              inner join pg_type tp on tp.oid = att.atttypid
               left join pg_attrdef adef on adef.adrelid = att.attrelid 
                                        and adef.adnum = att.attnum
              where att.attrelid = l_table_id
                and att.attnum > 0
              group by att.attrelid;
  
  att := trim(leading '  ,' from att);
  rez := rez||'   '||att||chr(10);

  --CONSTRAINT PRIMARY KEY
  if lrelreplident = 'd' then
    select concat('CONSTRAINT ', cnst.conname, ' PRIMARY KEY'), cnst.conkey 
      into lf1, l_conkey_arr
      from pg_constraint cnst 
     where cnst.conrelid = l_table_id 
       and cnst.contype = 'p';

    select concat(' (',string_agg(att.attname,','),')') 
      into lconkey
      from pg_attribute att 
     where att.attrelid = l_table_id
       and att.attnum = any(l_conkey_arr);

    rez := concat_ws(chr(10), rez, '  ,'||lf1||lconkey); -- rez||'  ,'||lf1||lconkey||chr(10);
  end if;
   
  --CONSTRAINT UNIQUE
  if exists (select 1 
               from pg_constraint cnst 
              where cnst.conrelid = l_table_id 
                and cnst.contype = 'u') then

    --rez:=rez||'  ,';
    for rec in select concat('  ,CONSTRAINT ', cnst.conname, ' UNIQUE') conname, 
                      cnst.conkey
                 from pg_constraint cnst 
                 left join pg_class cl on cl.oid = cnst.confrelid
                where cnst.conrelid = l_table_id
                  and cnst.contype = 'u'
    loop
      select concat(' (',string_agg(att.attname,',' order by att.attnum),') ',ldescr) 
        into lconkey
        from pg_attribute att 
       where att.attrelid = l_table_id 
         and att.attnum = any(rec.conkey);

      rez := rez||rec.conname||lconkey||chr(10);
    end loop;
  end if;  


  lconkey:=''; 
  --rez:=replace(rez, ',  ,CONSTRAINT',',CONSTRAINT');

  --CONSTRAINT FOREIGN KEY
  if exists (select 1 
               from pg_constraint cnst 
              where cnst.conrelid = l_table_id 
                and cnst.contype = 'f') then
    rez:=rez||'  ,';

    for rec in select concat('  ,CONSTRAINT ',cnst.conname, ' FOREIGN KEY') as f1
                         ,case cnst.confrelid 
                            when 0 then '' 
                            else concat('     REFERENCES ',p_schema_name,'.',cl.relname) 
                          end as f2
                         ,concat((case cnst.confmatchtype 
                                    when 'f' then 'MATCH FULL' 
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
                   where cnst.conrelid = l_table_id
                     and cnst.contype = 'f'
    loop
      select concat(' (',string_agg(att.attname,','),') ',ldescr) 
        into lconkey
        from pg_attribute att 
       where att.attrelid = l_table_id 
         and att.attnum = any(rec.conkey);

      select concat(' (',string_agg(att.attname,','),') ',ldescr) 
        into lconkeyf
        from pg_attribute att 
       where att.attrelid = rec.oid 
         and att.attnum = any(rec.confkey);

      rez:=rez||rec.f1||lconkey||chr(10)||rec.f2||lconkeyf||rec.f3||chr(10);
    end loop;
  end if;  

  lf1 := null;

  lconkey:=''; lconkeyf:=''; 
  rez:=replace(rez, ',  ,CONSTRAINT',',CONSTRAINT');
  				   
  --CONSTRAINT CHECK
  if lrelchecks then
    rez:=rez||'  ,';

    select concat('CONSTRAINT ',cnst.conname,' CHECK'), cnst.conkey, cnst.consrc
      into lf1, l_conkey_arr, ldescr
      from pg_constraint cnst 
     where cnst.conrelid = l_table_id 
       and cnst.contype = 'c';

    rez:=rez||lf1;

    select concat(' (',string_agg(att.attname,','),') ',ldescr) 
      into lconkey
      from pg_attribute att 
     where att.attrelid = l_table_id 
       and att.attnum = any(l_conkey_arr); --in (select conkey from tempp);

    rez:=rez||lconkey||chr(10);
  end if;

  lconkey:=''; 
  lf1:=null;
  ldescr:=null;		   

  if lrelhasoids then 
    rez:=rez||') WITH OIDS);'||chr(10); 
  else 
    rez:=rez||') WITH (OIDS = FALSE);'||chr(10)||chr(10); 
  end if;

  --COMMENTS
  if exists (select 1 
               from pg_description des 
              where des.objoid = l_table_id) then

    for rec in select des.objsubid, des.description 
                     ,case des.objsubid when 0 
                        then 'COMMENT ON TABLE '||p_schema_name||'.'||p_table_name||chr(10)||' IS '''||des.description||''';' 
                        else 'COMMENT ON COLUMN '||p_schema_name||'.'||p_table_name||'.'||att.attname||chr(10)||' IS '''||des.description||''';' 
                      end as f1
                 from pg_description des
                 left join pg_attribute att on att.attnum = des.objsubid 
                                           and att.attrelid = l_table_id
                where des.objoid = l_table_id
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
                   where idx.indrelid = l_table_id
                     and idx.indisprimary = false 
                   order by 1
    loop
      lconkey:=lconkey||'CREATE INDEX '||rec.relname||chr(10)||' ON '||l_table_name||chr(10)||' USING '||upper(rec.amname)||' ('||rec.indnatts||' key) '||'TABLESPACE pg_default;'||chr(10);
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
                     and tr.tgrelid = l_table_id
    loop
      lconkey:=lconkey||'CREATE TRIGGER '||rec.tgname||chr(10)||rec.f1||chr(10)||' ON '||p_schema_name||'.'||p_table_name||chr(10)||
               ' FOR EACH ROW '||chr(10)||'EXECUTE PROCEDURE '||p_schema_name||'.'||rec.proname||'('||rec.f2||');'||chr(10);
    end loop;
    rez:=rez||lconkey;lconkey:='';
  end if;

  --DATA
  if exists (select 1 
               from information_schema.tables 
              where table_schema = 'common'
                and table_name = 'spr_reposit')
  then 
    if exists (select 1 
               from common.spr_reposit 
              where nspname = p_schema_name 
                and relname = p_table_name) then
      
    drop table if exists tmpdat;
    create temporary table tmpdat(valchar varchar);  

    for rec in select att.attname, att.attnum,
                      row_number() over () rn
                 from pg_attribute att
                where att.attrelid = l_table_id
                  and att.attnum > 0
                  and att.atttypid <> 0
                order by att.attnum
    loop
      if rec.rn = 1 then 
        psort := rec.attname; 
      end if;
      pclmn := pclmn||', '||rec.attname;
      execute 'select max(coalesce(length('||rec.attname||'::varchar),4)) from '||l_table_name into pmaxlen;
      if pmaxlen is null then
        exit;
      end if;  
      pclmnnotype:=pclmnnotype||'rpad((coalesce('||rec.attname||'::varchar,''null'')),'||pmaxlen+2||')||'',''||';
    end loop;

    if pmaxlen is not null then
      pclmn:=trim(leading ', ' from pclmn);
      pclmnnotype:=trim(trailing '||'',''|| ' from pclmnnotype);
      pdata:='insert into '||l_table_name||'('||pclmn||') values '||chr(10);
      execute 'insert into tmpdat (valchar) (select '||pclmnnotype||' from '||l_table_name||' order by '||psort||')';
      for rec in select valchar from tmpdat
      loop
        pdata:=pdata||'( '||rec.valchar||'),'||chr(10);
      end loop;
    else
      pdata := 'Данные не найдены';
    end if;  
    rez:=rez||pdata;
    pdata:='';    
  end if;
  end if;

  --raise info 'rez-%',rez;

  return rez;

exception
  when others then
    return sqlerrm;
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