MATCH
  (: Field {uid: $field})<-[:IS_IN*]-(tree:Tree)
RETURN count(distinct(tree))