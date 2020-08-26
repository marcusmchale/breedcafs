MATCH
  (: Item {uid: $uid})<-[:IS_IN | FROM*]-(sample: Sample)
RETURN count(distinct(sample))