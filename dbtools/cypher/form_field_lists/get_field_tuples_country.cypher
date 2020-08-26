MATCH
  (:Country {
    name_lower: toLower($country)
  })<-[:IS_IN]-(:Region)
    <-[:IS_IN]-(:Farm )
    <-[:IS_IN]-(field: Field)
RETURN [
  field.uid,
  field.name
]
ORDER BY field.name