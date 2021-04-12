MATCH
  (: User {
    username_lower: toLower($username)
  })-[: AFFILIATED {
    admin: true
  }]->(p: Partner)
WITH p
MATCH
  (p)<-[a: AFFILIATED]-(u: User)
RETURN {
  Username: u.username,
  Email: u.email,
  Name: u.name,
  Partner: p.name,
  PartnerFullName: p.fullname,
  Confirmed: a.confirmed
}