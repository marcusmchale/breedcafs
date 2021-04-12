MATCH
  (: Field {uid: $field})
    <-[:IS_IN | FROM*]-(source_sample: Sample)
    <-[:IS_IN | FROM*]-(sample: Sample)
WHERE source_sample.id IN $samples
RETURN count(distinct(sample))
