MATCH
  (u: User {
    username_lower: toLower($username)
  })
  -[a: AFFILIATED]->(p: Partner)
OPTIONAL MATCH
  (p)<-[: AFFILIATED {admin: true}]-(admin: User)
RETURN DISTINCT
  p.name ,
  p.fullname ,
  a.confirmed as confirmed,
  a.data_shared as data_shared ,
  collect(admin.email) as admin_emails