-- Fresh benchmark database only: indexed backlog monitoring and drain checks.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '30s';
SELECT (SELECT count(*) FROM job_runs WHERE status = 'PENDING')
     + (SELECT count(*) FROM job_runs WHERE status = 'QUEUED')
     + (SELECT count(*) FROM job_runs WHERE status = 'RUNNING');
COMMIT;
