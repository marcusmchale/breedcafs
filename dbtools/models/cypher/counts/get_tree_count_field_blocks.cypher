MATCH
  (: Field {uid: $field})
    <-[:IS_IN*]-(block: Block)
    <-[:IS_IN *]-(tree: Tree)
WHERE block.id IN $blocks
RETURN count(distinct(tree))