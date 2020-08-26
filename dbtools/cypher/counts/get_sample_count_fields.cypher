MATCH
  (field: Field)
    <-[:IS_IN | FROM*]-(sample: Sample)
WHERE field.uid IN $fields
RETURN count(distinct(sample))