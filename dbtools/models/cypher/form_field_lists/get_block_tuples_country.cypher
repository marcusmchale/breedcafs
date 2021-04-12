MATCH
  (block:Block)
    -[:IS_IN*]->(:Country {name_lower: toLower($country)})
RETURN
  [block.uid, block.name]
ORDER BY block.name
