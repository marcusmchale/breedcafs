MATCH (block: Block)
RETURN [block.uid, block.name]
ORDER BY block.name