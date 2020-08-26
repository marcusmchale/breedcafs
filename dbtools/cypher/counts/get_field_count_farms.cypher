//Due to a bug (https://github.com/neo4j/neo4j/issues/12149) list expansion isn't working in where clause,
// A temporary workaround is to modify the list first
WITH
  [f in $farms | toLower(f)] as farm_list
MATCH
  (: Country {name_lower: toLower($country)})
    <-[:IS_IN]-(: Region {name_lower: toLower($region)})
    <-[:IS_IN]-(farm: Farm)
    <-[:IS_IN]-(field: Field)
//WHERE country.name_lower IN [i in $country_list | toLower(i)]
WHERE farm.name_lower IN farm_list
RETURN count(distinct(field))