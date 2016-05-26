-- Tracking the history of data fixups

-- was 1910 which was before date_from which broke AS validation
-- the text of the record made clear it should have been 2010
-- fixed in cider_20160525
--update record_context set date_to = "2010" where record_id = "RCR00836";

-- was 2 = family which broke AS validation coz it is in a parent child reln
-- the text of the record made clear it should have been a person
-- fixed in cider_20160525
--update record_context set rc_type = 3 where record_id = "RCR00997";

-- 18 of these in cider_20160524
-- 2 remain in cider_20160525
delete from record_context_relationship where record_context = related_entity;
