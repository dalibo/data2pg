-- tests_pg_populate.sql: Populate the source database
--
SET client_min_messages TO WARNING;

--------------------------------
set search_path=public,myschema1;

insert into myTbl1 select i, 'ABC', E'\\014'::bytea from generate_series (1,10000) as i;

insert into myTbl2 select i,'ABC', '2010-12-31'::date + i * '1 days'::interval from generate_series (1,1000) as i;

insert into "myTbl3" (col33) select generate_series(1000,1039,4)/100.0;

insert into myTbl4 select i,'FK...',i,1,'ABC' from generate_series (1,100) as i;

---------------------------------
set search_path=public,myschema2;

insert into myTbl1 select i, case when i%3 = 0 then 'ABC' when i%3 = 1 then 'DEF' else 'GHI' end, E'\\014'::bytea
  from generate_series (1,110000) as i;

insert into myTbl2 select i,'DEF', NULL from generate_series (1,2000) as i;

insert into "myTbl3" (col33) select generate_series(10000,10399,4)/100;

insert into myTbl5 values (1,'{"abc","def","ghi"}','{1,2,3}',NULL,'{}');
insert into myTbl5 values (2,array['abc','def','ghi'],array[3,4,5],array['2000/02/01'::date,'2000/02/28'::date],'{"id":1000, "c1":"abc"}');

insert into myTbl6 select i, point(i,1.3), '((0,0),(2,2))', circle(point(5,5),i),'((-2,-2),(3,0),(1,4))','10.20.30.40/27' from generate_series (1,800) as i;

insert into myschema2.myTbl7 select i from generate_series(0,10000) i;

insert into myschema2.myTbl8 (col81) values (0);

alter sequence mySchema2.mySeq1 restart 1000;

--------------------------------------
set search_path=public,"phil's schema3";
--
insert into "phil's tbl1" select i, 'AB''C', E'\\000'::bytea from generate_series (1,31000) as i;

insert into myTbl4 (col41) values (1), (2), (3);

insert into "myTbl2\" values (1,'ABC','2010-12-31'), (2,'DEF',NULL);

select nextval(E'"phil''s schema3"."phil''s seq\\1"');
select nextval(E'"phil''s schema3"."phil''s seq\\1"');

