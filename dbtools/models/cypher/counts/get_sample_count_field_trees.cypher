MATCH
  (: Field {uid: $field})
    <-[:IS_IN*]-(tree: Tree)
    <-[:IS_IN | FROM*]-(sample: Sample)
WHERE tree.id IN $trees
RETURN count(distinct(sample))