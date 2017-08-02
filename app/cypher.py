class Cypher():
	user_find="MATCH (user:User) WHERE user.username = {username} OR user.email = {email} RETURN user"
	username_find="MATCH (user:User) WHERE user.username = {username} RETURN user"
	email_find="MATCH (user:User) WHERE user.email = {email} RETURN user"
	confirm_email="MATCH (user:User) WHERE user.email = {email} SET user.confirmed = 'True'"
	user_register="MATCH (partner:Partner) WHERE partner.name = {partner} CREATE (user:User {username : {username}," \
		" password : {password}, email : {email}, name : {name},"\
		" confirmed : {confirmed}})-[r:Affiliated] -> (partner)"
	user_del="MATCH (user:User) WHERE user.username = {username} OR user.email = {email} DELETE user"
	upload_submit=" MATCH (user:User {username : {username}}) " \
		" LOAD CSV WITH HEADERS FROM {filename} as csvLine CREATE (user)-[r:Submitted { " \
		" submission_time : {submission_time}, submission_type : {submission_type} }] " \
		" -> (p:Plot {id:csvLine.plot_id, yield:csvLine.value}) "
