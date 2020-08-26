MATCH
  (country:Country)<-[:IS_IN]-(region: Region)
OPTIONAL MATCH
  (region)<-[:IS_IN]-(farm: Farm)
OPTIONAL MATCH
  (farm)<-[:IS_IN]-(field: Field)
OPTIONAL MATCH
  (field)
  <-[:IS_IN]-(:FieldTrees)
  <-[:FOR]-(field_tree_counter: Counter {name:"tree"})
WITH
  country,
  region,
  farm,
  {
    name: field.name,
    label: "Field",
    treecount: field_tree_counter.count
  } as fields
WITH
  country,
  region,
  {
    name: farm.name,
    label: "Farm",
    children: FILTER(field IN collect(fields) WHERE field["name"] IS NOT NULL)
  } as farms
WITH
  country,
  {
    name: region.name,
    label:"Region",
    children: FILTER(farm IN collect(farms) WHERE farm["name"] IS NOT NULL)
  } as regions
WITH
  {
    name: country.name,
    label:"Country",
    children: FILTER(region IN collect (regions) WHERE region["name"] IS NOT NULL)
  } as countries
RETURN countries