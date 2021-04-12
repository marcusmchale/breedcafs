MATCH
  (field:Field)
RETURN [
  field.uid,
  field.name
]
ORDER BY field.name