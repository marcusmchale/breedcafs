# breedcafs

## Web portal built in Python ([Flask](https://github.com/pallets/flask)) to [Neo4J](https://github.com/neo4j/neo4j) database for BreedCAFS project  
 * Register locations (country,region,farm,plot) and trees
   * Generates unique plot ID and thread safe tree count (concatenated to UID)
   * Exports [Field-Book](https://github.com/PhenoApps/Field-Book) **field.csv** 
 * Select traits from database to export into [Field-Book](https://github.com/PhenoApps/Field-Book) **trait.trt**
 * Upload collected data as csv
   * From [Field-Book](https://github.com/PhenoApps/Field-Book) export in  database format
 * User management
   * Registration/Login/Logout (including sessions)
   * Submissions are assigned to user with SUBMITTED_BY relationship
   * Files generated are also emailed to users
 * Visualisations
   * D3 powered graphs of database content on relevant pages:
     * Country/Region/Farm/Plot and tree count on location/tree registration page
       * Click-to-expand/collapse
     * Sample of recent user-submitted data on data upload page
 * Uses the official [Neo4J driver for python](https://github.com/neo4j/neo4j-python-driver)

