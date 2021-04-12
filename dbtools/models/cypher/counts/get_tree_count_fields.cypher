MATCH
  (field: Field)
    <-[:IS_IN *]-(tree: Tree)
WHERE field.uid IN $fields
RETURN count(distinct(tree))