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
	country_find="MATCH (country:Country {name : {country}}) RETURN country"
	country_add="MERGE (country:Country {name :{country}})  RETURN country"
	region_find="MATCH (:Country {name : {country}})<-[:IS_IN]-(region:Region { name:{region}}) RETURN region"
	region_add="MATCH (c:Country {name : {country}}) MERGE (c)<-[:IS_IN]-(region:Region { name:{region}}) RETURN region"
	farm_find='MATCH (:Country {name : {country}})<-[:IS_IN]-(:Region { name:{region}})' \
	 	'<-[:IS_IN]-(farm:Farm { name: {farm}}) RETURN farm'
	farm_add='MATCH (c:Country {name : {country}})<-[:IS_IN]-(r:Region {name : {region}}) MERGE (c)<-[:IS_IN]-(r) ' \
		'<-[:IS_IN]-(farm:Farm { name:{farm}}) RETURN farm'
	plot_find='MATCH (:Country {name : {country}})<-[:IS_IN]-(:Region { name:{region}})' \
	 	'<-[:IS_IN]-(:Farm { name: {farm}})<-[:IS_IN]-(plot:Plot { name: {plot}}) RETURN plot'
	plot_add='MATCH (c:Country {name : {country}})<-[:IS_IN]-(r:Region {name : {region}}) ' \
		' <-[:IS_IN]-(f:Farm { name: {farm}}) MERGE (c)<-[:IS_IN]-(r)<-[:IS_IN]-(f)<-[:IS_IN]-' \
		' (plot:Plot { name:{plot}}) RETURN plot'
	get_farms='MATCH (:Country { name:{country}})<-[:IS_IN]-(:Region { name:{region}})<-[:IS_IN]-' \
		'(f:Farm) RETURN properties (f)'
	get_plots='MATCH (:Country { name:{country}})<-[:IS_IN]-(:Region { name:{region}})<-[:IS_IN]' \
		'-(:Farm { name:{farm}})<-[:IS_IN]-(p:Plot) RETURN properties (p)'