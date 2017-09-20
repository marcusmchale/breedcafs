# breedcafs

## Web portal built in Python ([Flask](https://github.com/pallets/flask)) to [Neo4J](https://github.com/neo4j/neo4j) database for BreedCAFS project  
 * Register locations (country,region,farm,plot) and trees
   * Generates thread safe unique plot/tree/sample ID's (concatenated to UID for tree/sample)
   * Generates [Field-Book](https://github.com/PhenoApps/Field-Book) **field.csv** for added trees and optionally for a custom range.
 * Select traits from database to export into [Field-Book](https://github.com/PhenoApps/Field-Book) **trait.trt**
 * Upload collected data as csv
   * From [Field-Book](https://github.com/PhenoApps/Field-Book) export in  database format
 * User management
   * Registration/Login/Logout/Password reset (including sessions)
   * Submissions are assigned to user with time-stamped SUBMITTED_BY relationship
   * Files generated are emailed to users
   * WTForms form validation (including CSRF protection)
 * Visualisations
   * Forms updated by jQuery for ease of submission
   * D3 powered graphs of database content on relevant pages:
     * Country/Region/Farm/Plot and tree count on location/tree registration page
       * Click-to-expand/collapse
     * Sample of recent user-submitted data on data upload page
 * Uses the official [Neo4J bolt driver for python](https://github.com/neo4j/neo4j-python-driver)

