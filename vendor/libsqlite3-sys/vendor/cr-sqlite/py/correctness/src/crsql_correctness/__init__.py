import sqlite3

extension = '../../core/dist/crsqlite'


def connect(db_file, uri=False):
    c = sqlite3.connect(db_file, uri=uri)
    c.enable_load_extension(True)
    c.load_extension(extension)
    return c


def close(c):
    c.execute("select crsql_finalize()")
    c.close()


def get_site_id(c):
    return c.execute("SELECT crsql_site_id()").fetchone()[0]

def sync_left_to_right(l, r, since):
    print("sync_left_to_right")
    changes = l.execute(
        "SELECT * FROM crsql_changes WHERE db_version > ?", (since,))
    for change in changes:
        r.execute(
            "INSERT INTO crsql_changes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", change)
    r.commit()

min_db_v = 0
