MATCH
  (field: Field {uid: $field})
    <-[:IS_IN *]-(source_block: Block)
    <-[:IS_IN *]-(block: Block)
WHERE source_block.id IN $blocks
RETURN count(distinct(block))