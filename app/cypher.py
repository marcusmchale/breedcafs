#add submission time for all SUBMITTED with ON MATCH SET and ON CREATE SET

class Cypher():
	user_find = ('MATCH (user:User) '
	' WHERE user.username = $username '
	' OR user.email = $email '
	' RETURN user ')
	username_find = ('MATCH (user:User) '
		' WHERE user.username = $username '
		' RETURN user ')
	email_find = ('MATCH (user:User) '
		'WHERE user.email = $email '
		'RETURN user ')
	confirm_email = ('MATCH (user:User) '
		' WHERE user.email = $email '
		' SET user.confirmed = "True" ')
	user_register = ('MATCH (partner:Partner) '
		' WHERE partner.name = $partner '
		' CREATE (user:User {username : $username, '
		' password : $password, email : $email, name : $name,'
		' confirmed : $confirmed}) '
		' -[r:Affiliated] -> (partner) ')
	user_del = ('MATCH (user:User) '
		' WHERE user.username = $username '
		' OR user.email = $email '
		' DELETE user ')
	upload_submit = (' LOAD CSV WITH HEADERS FROM $filename as csvLine '
		' MATCH (u:User {username : $username}), '
		' (t:Tree {uid:csvLine.UID}), '
		' (tr:Trait {name:csvLine.trait}) '
		' MERGE (t)<-[:DATA_FROM]-(d:Data {value:csvLine.value,'
		' timestamp:csvLine.timestamp,'
		' person:csvLine.person, '
		' location:csvLine.location}) '
		' -[:DATA_FOR]->(tr)'
		' ON MATCH SET d.found ="TRUE" '
		' ON CREATE SET d.found = "FALSE" '
		' CREATE (u)-[:SUBMITTED { '
		' submission_time : $submission_time, '
		' submission_type : $submission_type }]'
		' ->(d)'
		' RETURN d.found')
	country_find = ('MATCH (country:Country {name : $country}) '
		' RETURN country ')
	country_add = ('MATCH (user:User {username:$username}) '
		' MERGE (country:Country {name :$country}) '
		' <-[:SUBMITTED]-(user) ' )
	region_find = ('MATCH (:Country {name : $country}) '
		'<-[:IS_IN]-(region:Region { name:$region}) '
		' RETURN region ')
	region_add = ('MATCH (user:User {username:$username}), '
		' (c:Country {name : $country}) '
		' MERGE (c)<-[:IS_IN]- '
		' (:Region { name:$region}) '
		' <-[:SUBMITTED]-(user)')
	farm_find = ('MATCH (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
	 	' <-[:IS_IN]-(farm:Farm { name: $farm}) '
	 	' RETURN farm ')
	farm_add = ('MATCH (user:User {username:$username}), '
		' (:Country {name : $country}) '
		' <-[:IS_IN]-(r:Region {name : $region}) '
		' MERGE (r) <-[:IS_IN]-(:Farm { name:$farm}) '
		' <-[:SUBMITTED]-(user)')
	plot_find = ('MATCH (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
	 	' <-[:IS_IN]-(:Farm { name: $farm}) '
	 	' <-[:IS_IN]-(plot:Plot { name: $plot}) '
	 	' RETURN plot.name as name')
#https://stackoverflow.com/questions/32040409/reliable-autoincrementing-identifiers-for-all-nodes-relationships-in-neo4j
	plot_add = ('MATCH (user:User {username:$username}), '
		' (:Country {name : $country})<-[:IS_IN]-(:Region {name : $region}) '
		' <-[:IS_IN]-(f:Farm { name: $farm}) '
		' MERGE (id:UniqueId{name:"Plots"}) '
		' ON CREATE SET id.count=1 '
		' ON MATCH SET id.count=id.count+1 '
		' MERGE (f)<-[:IS_IN]-(:Plot { name:$plot, uid:id.count}) '
		' <-[:SUBMITTED]-(user) ')
	trees_add = ('MATCH (user:User {username:$username}), '
		' (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region {name : $region}) ' 
		' <-[:IS_IN]-(:Farm { name: $farm}) '
		' <-[:IS_IN]-(p:Plot {name:$plot})' 
		' UNWIND range(1, toInt($count)) as counter ' 
		' MERGE (p)<-[:ID_COUNTER_FOR]-(id:UniqueId{name:"Trees"})' 
		' ON CREATE SET id.count=1 '
		' ON MATCH SET id.count=id.count+1'
		' MERGE (p)<-[:IS_IN]-(t:Tree {uid:(p.uid + "_" + id.count)}) '
		' <-[:SUBMITTED]-(user)'
		' RETURN [t.uid, p.uid, id.count]' )
	get_farms = ('MATCH (:Country { name:$country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
		' <-[:IS_IN]-(f:Farm) '
		' RETURN properties (f)')
	get_plots = ('MATCH (:Country { name:$country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
		' <-[:IS_IN]-(:Farm { name:$farm}) '
		' <-[:IS_IN]-(p:Plot) '
		' RETURN properties (p)')
