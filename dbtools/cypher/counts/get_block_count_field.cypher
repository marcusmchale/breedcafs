MATCH
  (: Field {uid: $field})<-[:IS_IN*]-(block:Block)
RETURN count(distinct(block))