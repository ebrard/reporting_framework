# Data model

## Record

- Implement composite key for id (idea: could be done on
the SQL part)

# Controllers

##  Backend database controllers

- Check for None usage in assignment
- Implement logging of changes

## Reporting data database controllers

- To do :
    1. Manage connection
    2. Open connection
    3. Trigger query from data model
    4. Retrieve and parse output data (use controller:parse_query_result)
    5. Load these into the execution records data model
    
# Persistence

# Main program

- Implement Full program end-to-end logic
    1. Design command line user interface
        - Trigger execution of a name report
        - Read list of report
        - Configure a new report from UI
        - Configure a new report from JSON
        - Test correctness of a report manually inserted 
    2. Implement end-to-end run of execution
- Implement std logging

# Open points

- Library for command line parsing ?
- controler_db compatibility check with MS SQL