MATCH
  (: Field {uid: $field})
    <-[:IS_IN*]-(block: Block)
    <-[:IS_IN*]-(tree: Tree)
    <-[:IS_IN | FROM*]-(sample: Sample)
WHERE tree.id IN $trees AND block.id IN $blocks
RETURN count(distinct(sample))
