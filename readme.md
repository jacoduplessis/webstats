# webstats

Web analytics from your nginx logs
with some help from cloudflare.

## notes

- sqlite inserts with journal_mode=wal and synchronous=normal
can do about 100_000 inserts per second - fast enough
- no regex to improve parsing speed - custom log format
- caching ua parsing make a huge difference
- nginx can log to multiple files - log to temp/stats.log and 
truncate after each read

## todo

- graphs
- single html doc reports
- add index to date column and query on demand

