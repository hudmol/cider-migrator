
-- fixed in cider_20160525
--update record_context set date_to = "2010" where record_id = "RCR00836";

-- fixed in cider_20160525
--update record_context set rc_type = 3 where record_id = "RCR00997";

delete from record_context_relationship where record_context = related_entity;
