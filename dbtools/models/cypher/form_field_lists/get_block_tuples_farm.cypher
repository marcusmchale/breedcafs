MATCH
  (block:Block)
    -[:IS_IN*]->(:Farm {name_lower: toLower($farm)})
    -[:IS_IN]->(:Region {name_lower: toLower($region)})
    -[:IS_IN]->(:Country {name_lower: toLower($country)})
RETURN
  [block.uid, block.name]
ORDER BY block.name
