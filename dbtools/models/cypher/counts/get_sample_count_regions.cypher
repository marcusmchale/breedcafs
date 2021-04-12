//Due to a bug (https://github.com/neo4j/neo4j/issues/12149) list expansion isn't working in where clause,
// A temporary workaround is to modify the list first
WITH
  [r in $regions | toLower(r)] as region_list
MATCH
  (: Country {name_lower: toLower($country)})
    <-[:IS_IN]-(region: Region)
    <-[:IS_IN]-(:Farm)
    <-[:IS_IN | FROM*]-(sample: Sample)
//WHERE country.name_lower IN [i in $country_list | toLower(i)]
WHERE region.name_lower IN region_list
RETURN count(distinct(sample))