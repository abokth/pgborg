# Installation

Consider installing psycopg2 and systemd using distro packages:

    yum install python3-psycopg2 python3-systemd # or similar depending on distro

It's recommended to create a new Python virtual environment:

    python3 -m venv --system-site-packages /opt/pgborg

Install pgborg:

    /opt/pgborg/bin/pip install git+https://github.com/abokth/pgborg.git

Create two borg repositories, one for continuous archiving and one for regular backups.

In /etc/pgca.ini and /etc/pgdump.ini configure access to each of the repositories:

    [environment]
    BORG_REPO=...

Other variables (such as BORG_PASSCOMMAND and BORG_RSH) may also be added.

Configure the service unit file /etc/systemd/system/pgcad.service as:

    [Unit]
    Description=PostgreSQL Continuous Archiving
    After=network.target
    
    [Service]
    Type=notify
    Environment=PYTHONUNBUFFERED=1
    ExecStart=/opt/pgborg/bin/pgcad
    # These need to be a long enough period to allow the initial full backup.
    WatchdogSec=1200
    TimeoutSec=1200
    Restart=on-failure
    
    [Install]
    WantedBy=multi-user.target

Configure the service unit file /etc/systemd/system/pgdumpd.service as:

    [Unit]
    Description=PostgreSQL Periodic Full Backup
    After=network.target
    
    [Service]
    Type=notify
    Environment=PYTHONUNBUFFERED=1
    ExecStart=/opt/pgborg/bin/pgdumpd
    # These need to be a long enough period to allow the initial full backup.
    WatchdogSec=1200
    TimeoutSec=1200
    Restart=on-failure
    
    [Install]
    WantedBy=multi-user.target

Enable and start the repositories.
