# OctoSQL ElasticSearch Plugin

This plugin adds ElasticSearch support to OctoSQL.

## Installation

```
octosql plugin install elasticsearch
```

## Configuration

The available configuration variables are:
- host
- port
- user
- password
- database

An example octosql.yml file would be:
```yaml
databases:
  - name: mydb
    type: elasticsearch
    config:
      host: localhost
      port: 3306
      database: mydatabase
      user: myuser
      password: mypassword
```

You can also set the OCTOSQL_ELASTICSEARCH_QUERY_LOGGING environment variable to 1 to enable detailed query logging. They will then be visible in your `~/.octosql/logs.txt` file.

## Usage

After configuring a database as described above you can use tables from the configured database in your OctoSQL queries:
```
octosql "SELECT * FROM mydb.mytable" --describe
octosql "SELECT COUNT(*) FROM mydb.mytable"
```
