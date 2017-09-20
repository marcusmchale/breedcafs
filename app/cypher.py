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
	password_reset = ('MATCH (user:User) '
		' WHERE user.email = $email '
		' SET user.password = $password ')
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
		' timeFB:csvLine.timestamp,'
		' person:csvLine.person, '
		' location:csvLine.location}) '
		' -[:DATA_FOR]->(tr)'
		' ON MATCH SET d.found = "TRUE" '
		' ON CREATE SET d.found = "FALSE" '
		' CREATE (u)-[:SUBMITTED {timeInt : timestamp(), '
		' submission_type : $submission_type }]'
		' ->(d)'
		' RETURN d.found')
	country_find = ('MATCH (country:Country {name : $country}) '
		' RETURN country ')
	country_add = ('MATCH (user:User {username:$username}) '
		' MERGE (country:Country {name :$country}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user) ' )
	region_find = ('MATCH (:Country {name : $country}) '
		'<-[:IS_IN]-(region:Region { name:$region}) '
		' RETURN region ')
	region_add = ('MATCH (user:User {username:$username}), '
		' (c:Country {name : $country}) '
		' MERGE (c)<-[:IS_IN]- '
		' (:Region { name:$region}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user)')
	farm_find = ('MATCH (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
	 	' <-[:IS_IN]-(farm:Farm { name: $farm}) '
	 	' RETURN farm ')
	farm_add = ('MATCH (user:User {username:$username}), '
		' (:Country {name : $country}) '
		' <-[:IS_IN]-(r:Region {name : $region}) '
		' MERGE (r) <-[:IS_IN]-(:Farm { name:$farm}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user)')
	plot_find = ('MATCH (:Country {name : $country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
	 	' <-[:IS_IN]-(:Farm { name: $farm}) '
	 	' <-[:IS_IN]-(plot:Plot { name: $plot}) '
	 	' RETURN plot.name as name')
#for autoincrement:
# https://stackoverflow.com/questions/32040409/reliable-autoincrementing-identifiers-for-all-nodes-relationships-in-neo4j
# for lock:
#this allows the increment counter (allowing for concurrent transactions to be serialised):
#http://neo4j.com/docs/stable/transactions-isolation.html
#https://stackoverflow.com/questions/35138645/how-to-perform-an-atomic-update-on-relationship-properties-with-py2neo
#https://stackoverflow.com/questions/31798311/write-lock-behavior-in-neo4j-cypher-over-transational-rest-ap
#This was tested by running many threads on the same operation (plot_add)
#without plot_id_lock (as a separate cypher query) it fails to maintain the count properly and clashes with the unique UID constraint
	plot_id_lock = (' MERGE (id:PlotID{name:"Plots"}) '
		' ON CREATE SET id._LOCK_ = true, id.count=0 '
		' ON MATCH SET id._LOCK_ = true ')
	plot_add = ('MATCH (user:User {username:$username}), '
		' (:Country {name : $country})<-[:IS_IN]-(:Region {name : $region}) '
		' <-[:IS_IN]-(f:Farm { name: $farm}) '
		' MATCH (id:PlotID{name:"Plots"}) '
		' SET id.count=id.count+1 '
		' MERGE (f)<-[:IS_IN]-(:Plot { name:$plot, uid:id.count}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user) '
		' SET id._LOCK_ = false')
	tree_id_lock = (' MATCH  (p:Plot {uid: $plotID})'
		' MERGE (p)<-[:ID_COUNTER_FOR]-(id:TreeID{plotID:p.uid})' 
		' ON CREATE SET id._LOCK_ = true, id.count = 0 '
		' ON MATCH SET id._LOCK_ = true ')
	trees_add = ('MATCH (user:User {username:$username}), '
		' (c:Country)<-[:IS_IN]-(r:Region)<-[:IS_IN]-(f:Farm)'
		' <-[:IS_IN]-(p:Plot {uid: $plotID})' 
		' MATCH (id:TreeID {plotID:p.uid}) '
		' UNWIND range(1, toInt($count)) as counter ' 
		' SET id.count=id.count+1 '
		' MERGE (p)<-[:IS_IN]-(t:Tree {uid:(p.uid + "_" + id.count), id:id.count}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user)'
		' SET id._LOCK_ = false '
		' RETURN [t.uid, p.uid, t.id, p.name, f.name, r.name, c.name]'
		' ORDER BY t.id')
	trees_get = ('MATCH (c:Country)<-[:IS_IN]'
		' -(r:Region)<-[:IS_IN]-(f:Farm)<-[:IS_IN]-'
		' (p:Plot {uid: $plotID})<-[:IS_IN]-(t:Tree)'
		' WHERE t.id>=$start '
		' AND t.id<=$end '
		' RETURN [t.uid, p.uid, t.id, p.name, f.name, r.name, c.name]'
		' ORDER BY t.id')
	get_farms = ('MATCH (:Country { name:$country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
		' <-[:IS_IN]-(f:Farm) '
		' RETURN properties (f)')
	get_plots = ('MATCH (:Country { name:$country}) '
		' <-[:IS_IN]-(:Region { name:$region}) '
		' <-[:IS_IN]-(:Farm { name:$farm}) '
		' <-[:IS_IN]-(p:Plot) '
		' RETURN properties (p)')
	get_submissions_range = ('MATCH (:User {username:$username})'
		' -[s:SUBMITTED]->(d)'
		' WHERE s.timeInt>=$starttimeInt AND s.timeInt<=$endtimeInt'
		' MATCH (d)-[r]-(n)'
		' WHERE NOT type (r) IN ["SUBMITTED","ID_COUNTER_FOR"] '
		' RETURN '
			' labels(d)[0] as d_label, '
			' coalesce(d.name,d.uid,d.value) as d_name,'
			' id(d) as d_id, '
			' labels(n)[0] as n_label, '
			' coalesce(n.name,n.uid,n.value) as n_name,'
			' id(n) as n_id, '
			' id(r) as r_id, '
			' type(r) as r_type, '
			' id(startNode(r)) as r_start, '
			' id(endNode(r)) as r_end '
		'LIMIT 100')
	get_plots_treecount = (' '
		' MATCH (C:Country) '
		' OPTIONAL MATCH (C) <-[a:IS_IN]-(R:Region) '
		' OPTIONAL MATCH (R) <-[b:IS_IN]-(F:Farm) '
		' OPTIONAL MATCH (F) <-[c:IS_IN]-(P:Plot) '
		' OPTIONAL MATCH (P) <-[d:ID_COUNTER_FOR]-(T:TreeID) '
		' RETURN '
			' labels(C)[0] as C_label, '
			' labels(R)[0] as R_label, '
			' labels(F)[0] as F_label, '
			' labels(P)[0] as P_label, '
			' C.name, R.name, F.name, P.name, '
			' P.uid, T.count ')
	tissue_add = ('MATCH (user:User {username:$username}) '
		' MERGE (tissue:Tissue {name :$tissue}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user) ' )
	storage_add = ('MATCH (user:User {username:$username}) '
		' MERGE (storage:Storage {name :$storage}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user) ' )
	sample_id_lock = (' MATCH  (p:Plot {uid: $plotID})'
		' MERGE (p)<-[:ID_COUNTER_FOR]-(id:SampleID{plotID:p.uid})' 
		' ON CREATE SET id._LOCK_ = true, id.count = 0 '
		' ON MATCH SET id._LOCK_ = true ')
	samples_add = ('MATCH (p:Plot {uid: $plotID})<-[:IS_IN]-(t:Tree)' 
		' WITH t '
		' ORDER BY t.id '
		' WHERE t.id >= $start '
		' AND t.id <= $end '
		' MATCH (user:User {username:$username}), '
		' (tissue:Tissue {name:$tissue}), '
		' (storage:Storage {name:$storage}), '
		' (c:Country)<-[:IS_IN]-(r:Region)<-[:IS_IN]-(f:Farm)'
		' <-[:IS_IN]-(p:Plot {uid: $plotID})<-[:IS_IN]-(t),' 
		' (id:SampleID {plotID:p.uid} ) '
		' SET id.count=id.count+1 '
		' MERGE (t)<-[:SAMPLE_FROM]-(s:Sample {uid:(t.uid + "_" + id.count), id:id.count, date:$date}) '
		' -[:SAMPLE_OF]->(tissue)'
		' MERGE (s)-[:STORED_IN {stored_on:$date}]->(storage)'
		' RETURN [s.uid, p.uid, t.id, s.id, s.date, tissue.name, storage.name, p.name, f.name, r.name, c.name] '
		' ORDER BY s.id')
	soil_add = ('MATCH (user:User {username:$username}) '
		' MERGE (:Soil {name :$soil}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user) ' )
	shade_tree_add = ('MATCH (user:User {username:$username}) '
		' MERGE (:ShadeTree {name :$shade_tree}) '
		' <-[:SUBMITTED {timeInt : timestamp()}]-(user) ' )
	field_details_update = ('MATCH (user:User {username:$username}), '
		' (p:Plot {plotID:$plotID}) '
		' MERGE (p)-[h:HAS PROPERTY]-> (:Soil {name:$soil})'
		' ON CREATE SET  '
		' ON MATCH SET ')