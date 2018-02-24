# Reporting Framework

## Description

Run a SQL command in a data source to produce a report
as a plain text file (Symbol separated file). In full mode, 
always generate a full report. In delta mode, generate a diff
between the current and the previous execution identifying
deleted, modified and created record.

## To-Do

- Implement loading of report configuration from file
- Implement controller for external source DB
- Implement database technology abstraction to handle difference kind of DB

- Design an endpoint: 
	- Ressources:
		- /api/v1/reports
			- GET all
		- /api/v1/reports?name=name
			- Get by name
		- /api/v1/execution
			- /api/v1/execution?report_name=name
			- POST
				{"report_name": "name",
				 "mode": "delta",
				}