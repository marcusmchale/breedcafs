MATCH
  (:Country {
    name_lower: toLower($country)
  })<-[:IS_IN]-(:Region {
    name_lower: toLower($region)
  })<-[:IS_IN]-(:Farm {
    name_lower: toLower($farm)
  })<-[:IS_IN]-(field:Field)
RETURN [
  field.uid,
  field.name
]
ORDER BY field.name