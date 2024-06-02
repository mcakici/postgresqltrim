## Trim All Varchar Columns In Database

- It supports MsSQL and PostgreSQL databases.
- Database connection and parallelism settings can be made from config.json file.
- config.json file should be located in the main directory of the project you are running.

#### Sample config file for postgreSQL;

```
{
    "database": "dbname",
    "user": "dbusername",
    "password": "dbpass",
    "host": "localhost",
    "port": "5432",
    "max_workers": 4,
    "db_type": "postgresql"
}
```



#### If you want to recreate the single exe file;
`
pyinstaller --onefile  TrimColumnsInDb.py
`