# pgborg

## Services and utilities for continous archiving and regular full backups of PostgreSQL

### Continuous archiving

This implements [PostgreSQL Continuous Archiving and Point-in-Time Recovery](https://www.postgresql.org/docs/current/continuous-archiving.html).

#### pgcad - Continuous archiving of PostgreSQL servers into a Borg archive

A daemon which connects to all PostgreSQL instances on the host and implements continuous archiving with a Borg archive as backend.

- Automatically creates new snapshots when appropriate.
- Automatically manages backup mode.
- Snapshots the data directory using filesystem level copy-on-write.
- Manages WAL archives.

No temporary files except WAL archives, meaning minimal disk write activity and temporary disk space requirements.

WAL archives are stored associated with a base snapshot. More than one base snapshot and its WAL archives are managed concurrently and the previous base snapshots and the WAL archiving only ends after a new base snapshot has been completed and its associated WALs have started to be archived. This ensure there are no gaps in the continuous archiving and makes cleanup of old archives simple (since WALs are duplicated around the time of the snapshot).

#### pgbuptool - Create restorepoints usable with pgcad/pgcarestore

Manage restorepoints which can be used as reference in pgcarestore (below).

#### pgcarestore - Restore backups from pgcad

Restores an entire database from the archive, either the latest of at a specific restorepoint or a specific time. WALs are extracted and made available to the database during the recovery phase.

    # systemctl stop pgcad
    # systemctl stop postgresql
    # rm -rf /var/lib/pgsql/data
    # pgcarestore restore
    # systemctl start postgresql
    # systemctl start pgcad

### Regular backups

This implements regular backups.

#### pgdumpd - Regular full backups of PostgreSQL servers into a Borg archive

A daemon which connects to all PostgreSQL instances on the host and regularly performs full backups of each database in each instance with a Borg archive as backend. Data is streamed directly from pg_dump to Borg, meaning minimal disk write activity and temporary disk space requirements.

#### pgdumprestore - Restore backups from pgdumpd

Retrieves backups from the archive, either the latest backup or based on a date and time. Data is streamed to stdout or fed into the given command, which will be run as the PostgreSQL service user.

Example:

    # cd $(mktemp -d)
    # pgdumprestore extract mydb -- pg_restore -l >list-file
    # $EDITOR list-file
    # pgdumprestore extract mydb -- pg_restore -L list-file -c -d mydb
