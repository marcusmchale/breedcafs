MATCH
  (block: Blocks)-[:IS_IN*]->(item: Item {uid: $uid})
RETURN
  [block.uid, block.name]
ORDER BY block.name
