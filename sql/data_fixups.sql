-- Tracking the history of data fixups

-- was 1910 which was before date_from which broke AS validation
-- the text of the record made clear it should have been 2010
-- fixed in cider_20160525
-- update record_context set date_to = "2010" where record_id = "RCR00836";

-- was 2 = family which broke AS validation coz it is in a parent child reln
-- the text of the record made clear it should have been a person
-- fixed in cider_20160525
-- update record_context set rc_type = 3 where record_id = "RCR00997";

-- 18 of these in cider_20160524
-- 2 remain in cider_20160525
-- fixed in cider_20160527
-- select count(*) from record_context_relationship where record_context = related_entity;
-- delete from record_context_relationship where record_context = related_entity;

-- 538 collections don't have a date in cider_20160527
-- resources need a date. what to do?
-- now generating date 1453 in converter
-- select count(obj.number) from collection col, object obj where col.id = obj.id and col.bulk_date_from is NULL and col.bulk_date_to is NULL;
-- update collection set bulk_date_from = '1970' where bulk_date_from is NULL and bulk_date_to is NULL;

-- seven collections don't have any locations in cider_20160527
-- resources must have at least one extent, what to do?
-- now generating fake extent in converter
-- select obj.number, col.id from collection col, object obj where col.id = obj.id and col.id not in (select object from object_location);

-- starting to look at building trees under resources in cider_20160527
-- found two objects that don't have a parent and aren't linked to a collection
-- select * from object where parent is null and id not in (select id from collection);
-- looking at objects with similar numbers
-- select * from object where number like 'UP006%';
-- it seems likely these guys should have a parent of 32378, so
-- update object set parent = 32378  where parent is null and id not in (select id from collection);
-- fixed in cider_20160608

-- SUBJECTs

-- in cider_20160527 there is an authority_name with a null name - AS needs a value here
-- select * from authority_name where name is null;
-- id = 24992
-- unfortunately it is used:
-- select o.number from object o, item_authority_name i where i.item = o.id and i.name = 24992;
-- let's just give it a value for now:
-- update authority_name set name = 'WATCH OUT: made up by importer' where name is null;
-- fixed in cider_20160720

-- geographic_term and topic_term are fine:
-- select * from geographic_term where name is null;
-- select * from topic_term where name is null;

-- FAAASTER
create index do_pid_idx on digital_object (pid(50));

-- fix date data
-- 19974	NULL
update item set item_date_from = '1997' where item_date_from = '19974' and item_date_to is null;
-- 1971	19766
update item set item_date_to = '1976' where item_date_from = '1971' and item_date_to = '19766';
-- 199110-05	NULL
update item set item_date_from = '1991-10-05' where item_date_from = '199110-05' and item_date_to is null;
-- 19791	1981
update item set item_date_from = '1979' where item_date_from = '19791' and item_date_to = '1981';
-- 19561977	NULL
update item set item_date_from = '1956' and item_date_to = '1977' where item_date_from = '19561977' and item_date_to is null;
-- 191957	1971
update item set item_date_from = '1957' where item_date_from = '191957' and item_date_to = '1971';
-- 1969	19979
update item set item_date_to = '1979' where item_date_from = '1969' and item_date_to = '19979';
-- 20014	NULL
update item set item_date_from = '2001-04' where item_date_from = '20014' and item_date_to is null;
-- 2000	20004
update item set item_date_to = '2004' where item_date_from = '2000' and item_date_to = '20004';
-- 1986	19870
update item set item_date_to = '1987' where item_date_from = '1986' and item_date_to = '19870';
-- 2000	20001
update item set item_date_to = '2001' where item_date_from = '2000' and item_date_to = '20001';
-- 19228	NULL
update item set item_date_from = '1928' where item_date_from = '19228' and item_date_to is null;
-- 19554	NULL
update item set item_date_from = '1954' where item_date_from = '19554' and item_date_to is null;
-- 2000	20001
update item set item_date_to = '2001' where item_date_from = '2000' and item_date_to = '20001';
-- 19458	NULL
update item set item_date_from = '1948' where item_date_from = '19458' and item_date_to is null;
-- 1984	19990
update item set item_date_to = '1999' where item_date_from = '1984' and item_date_to = '19990';
-- 1974	19997
update item set item_date_to = '1997' where item_date_from = '1974' and item_date_to = '19997';
-- 19886	NULL
update item set item_date_from = '1986' where item_date_from = '19886' and item_date_to is null;
-- 19633-06	NULL
update item set item_date_from = '1963-06' where item_date_from = '19633-06' and item_date_to is null;
-- 19950-06	NULL
update item set item_date_from = '1995-06' where item_date_from = '19950-06' and item_date_to is null;