MATCH
  (: Field {uid: $field})
    <-[:IS_IN*]-(block: Block)
    <-[:IS_IN | FROM*]-(sample: Sample)
WHERE block.id IN $blocks
RETURN count(distinct(sample))