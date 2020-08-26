MATCH
  (field: Field)
    <-[:IS_IN *]-(block: Block)
WHERE field.uid IN $fields
RETURN count(distinct(block))