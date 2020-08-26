MATCH
  (e: Emails)
UNWIND e.allowed as emails
RETURN DISTINCT
  emails