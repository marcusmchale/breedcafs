MATCH
  (field: Field)
RETURN count(distinct(field))