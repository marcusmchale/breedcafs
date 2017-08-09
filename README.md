# breedcafs

## Web portal (Flask) to Neo4J database for BreedCAFS project  
 * Register locations (country,region,farm,plot)
   * Generates plot ID and builds unique IS_IN relationships
 * Register trees (generate ID) and exports [Field-Book](https://github.com/PhenoApps/Field-Book) **field.csv** 
 * Select traits from database to export into [Field-Book](https://github.com/PhenoApps/Field-Book) **trait.trt**
 * Upload collected data as csv
   * From [Field-Book](https://github.com/PhenoApps/Field-Book) export in  database format
 * User management
   * Registration/Login/Logout (including sessions)
   * Submissions are assigned to user with SUBMITTED_BY relationship
   * Files generated are also emailed to users
