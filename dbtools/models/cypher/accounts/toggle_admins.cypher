UNWIND $users_partners as user_partner
MATCH
  (p: Partner {
    name_lower: toLower(user_partner[1])
  })<-[a: AFFILIATED]-(u: User {
    username_lower: toLower(user_partner[0])
  })
SET
  a.admin = NOT a.admin
WITH u
MATCH (u)-[a:AFFILIATED]->(:Partner)
WITH u, collect(a.admin) as admin_rights
set u.access = CASE
WHEN
"global_admin" IN u.access AND true IN admin_rights
THEN ["user", "partner_admin", "global_admin"]
WHEN
"global_admin" IN u.access
THEN ["user", "global_admin"]
WHEN
true IN admin_rights
THEN ["user","partner_admin"]
ELSE
["user"]
END
RETURN
u.name