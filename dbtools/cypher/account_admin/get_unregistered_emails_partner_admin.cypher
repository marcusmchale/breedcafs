MATCH
  (u: User)
WITH
  COLLECT (DISTINCT u.email) as registered_emails
MATCH
  (user: User {username_lower : toLower($username)})
  -[: SUBMITTED]->(: Submissions)
  -[: SUBMITTED]->(e: Emails)
RETURN
[n in e.allowed WHERE NOT n in registered_emails]