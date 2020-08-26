MATCH
  (: Field {uid: $field})<-[:IS_IN | FROM*]-(sample: Sample)
RETURN count(distinct(sample))