# Data model

## Logical model

- Change write report function to execution or controller
but anyhow remove it from report.
- Add business_key flag to report column definition

## Physical implementation

- Report table, add unique constrain on name since
name will be our unique report identifier

## Record

- Implement composite key for id (idea: could be done on
the SQL part)

# Controllers

## Main controllers

- Create a function to handle composite business key``

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

# Bugs

- Misconception between Record.id which is a technical key
and the business key which is missing from the physical model
- Retrieval of db data must be always in string and stored as string