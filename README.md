# task 1

### Steps to validate task 1

- navigate to task one folder
```sh
cd .\task_one\
```
- start airflow, postgre in docker
```sh
docker compose up
```
- connect to locally runnning postgresql accroding to creds in **load_csv.sh** and run following script:
```sql
select * from public.events
```