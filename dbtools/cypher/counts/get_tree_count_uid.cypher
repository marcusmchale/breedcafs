MATCH
  (: Item {uid: $uid})<-[:IS_IN*]-(tree:Tree)
RETURN count(distinct(tree))