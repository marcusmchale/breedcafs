MATCH
  (: Item {uid: $uid})<-[:IS_IN*]-(block:Block)
RETURN count(distinct(block))