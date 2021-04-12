MATCH
  (sample: Sample)
RETURN count(distinct(sample))