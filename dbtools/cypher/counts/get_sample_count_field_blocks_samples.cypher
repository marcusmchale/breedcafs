MATCH
  (: Field {uid: $field})
    <-[:IS_IN*]-(block: Block)
    <-[:IS_IN | FROM*]-(source_sample: Sample)
    <-[:IS_IN | FROM*]-(sample: Sample)
WHERE source_sample.id IN $samples
  AND block.id IN $blocks
RETURN count(distinct(sample))
