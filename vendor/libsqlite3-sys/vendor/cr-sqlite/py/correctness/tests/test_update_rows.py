from crsql_correctness import connect

def sync_left_to_right(l, r, since):
    changes = l.execute(
        "SELECT * FROM crsql_changes WHERE db_version > ?", (since,))
    for change in changes:
        r.execute(
            "INSERT INTO crsql_changes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", change)
    r.commit()

def test_update_pk():
    def create_db():
        db = connect(":memory:")
        db.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY NOT NULL, a, b)")
        db.execute("SELECT crsql_as_crr('foo');")
        db.commit()
        return db

    def get_site_id(db):
        return db.execute("SELECT crsql_site_id()").fetchone()[0]

    db1 = create_db()
    db2 = create_db()

    db1_site_id = get_site_id(db1)
    db2_site_id = get_site_id(db2)

    db1.execute("INSERT INTO foo (id, a, b) VALUES (1, 2, 3)")
    db1.commit()

    db1.execute("INSERT INTO foo (id, a, b) VALUES (2, 5, 6)")
    db1.commit()

    sync_left_to_right(db1, db2, 0)

    db1_changes = db1.execute("SELECT * FROM crsql_changes").fetchall()

    assert (db1_changes == [('foo', b'\x01\t\x01', 'a', 2, 1, 1, db1_site_id, 1, 0, '0'),
                    ('foo', b'\x01\t\x01', 'b', 3, 1, 1, db1_site_id, 1, 1, '0'),
                    ('foo', b'\x01\t\x02', 'a', 5, 1, 2, db1_site_id, 1, 0, '0'),
                    ('foo', b'\x01\t\x02', 'b', 6, 1, 2, db1_site_id, 1, 1, '0')])

    db2_changes = db2.execute("SELECT * FROM crsql_changes").fetchall()
    assert (db2_changes == db1_changes)

    # update primary key
    db1.execute("UPDATE foo SET id = 10 WHERE id = 1")
    db1.commit()

    db1_foo = db1.execute("SELECT * FROM foo").fetchall()
    assert (db1_foo == [(2, 5, 6), (10, 2, 3)])

    db1_changes = db1.execute("SELECT * FROM crsql_changes").fetchall()
    assert (db1_changes == [('foo', b'\x01\t\x02', 'a', 5, 1, 2, db1_site_id, 1, 0, '0'),
                ('foo', b'\x01\t\x02', 'b', 6, 1, 2, db1_site_id, 1, 1, '0'),
                ('foo', b'\x01\t\x01', '-1', None, 2, 3, db1_site_id, 2, 0, '0'),
                ('foo', b'\x01\t\n', '-1', None, 1, 3, db1_site_id, 1, 1, '0'),
                ('foo', b'\x01\t\n', 'a', 2, 2, 3, db1_site_id, 1, 2, '0'),
                ('foo', b'\x01\t\n', 'b', 3, 2, 3, db1_site_id, 1, 3, '0')])

    sync_left_to_right(db1, db2, 2)

    db2_changes = db2.execute("SELECT * FROM crsql_changes").fetchall()
    assert (db2_changes == db1_changes)

    db2_foo = db2.execute("SELECT * FROM foo").fetchall()
    assert (db2_foo == db1_foo)


def test_empty_update_doesnt_change_db_version():
    def create_db():
        db = connect(":memory:")
        db.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY NOT NULL, a, b)")
        db.execute("SELECT crsql_as_crr('foo');")
        db.commit()
        return db

    def get_site_id(db):
        return db.execute("SELECT crsql_site_id()").fetchone()[0]

    db1 = create_db()
    db2 = create_db()

    db1_site_id = get_site_id(db1)
    db2_site_id = get_site_id(db2)

    db1.execute("INSERT INTO foo (id, a, b) VALUES (1, 2, 3)")
    db1.commit()

    db1.execute("INSERT INTO foo (id, a, b) VALUES (2, 5, 6)")
    db1.commit()

    sync_left_to_right(db1, db2, 0)

    db1_changes = db1.execute("SELECT * FROM crsql_changes").fetchall()

    assert (db1_changes == [('foo', b'\x01\t\x01', 'a', 2, 1, 1, db1_site_id, 1, 0, '0'),
                    ('foo', b'\x01\t\x01', 'b', 3, 1, 1, db1_site_id, 1, 1, '0'),
                    ('foo', b'\x01\t\x02', 'a', 5, 1, 2, db1_site_id, 1, 0, '0'),
                    ('foo', b'\x01\t\x02', 'b', 6, 1, 2, db1_site_id, 1, 1, '0')])

    db2_changes = db2.execute("SELECT * FROM crsql_changes").fetchall()
    assert (db2_changes == db1_changes)

    db1_db_version = db1.execute("SELECT crsql_db_version()").fetchone()[0]
    assert (db1_db_version == 2)

    # update row
    db1.execute("INSERT INTO foo (id, a, b) VALUES (2, 5, 6) ON CONFLICT (id) DO UPDATE SET a = 5, b = 6")
    db1.commit()

     # update row
    db1.execute("INSERT INTO foo (id, a, b) VALUES (1, 2, 3) ON CONFLICT (id) DO UPDATE SET a = 2, b = 3")
    db1.commit()

    db1_db_version = db1.execute("SELECT crsql_db_version()").fetchone()[0]
    assert (db1_db_version == 2)

    db1_changes = db1.execute("SELECT * FROM crsql_changes").fetchall()
    assert (db1_changes == [('foo', b'\x01\t\x01', 'a', 2, 1, 1, db1_site_id, 1, 0, '0'),
                ('foo', b'\x01\t\x01', 'b', 3, 1, 1, db1_site_id, 1, 1, '0'),
                ('foo', b'\x01\t\x02', 'a', 5, 1, 2, db1_site_id, 1, 0, '0'),
                ('foo', b'\x01\t\x02', 'b', 6, 1, 2, db1_site_id, 1, 1, '0')])

    # do an actual update
    db1.execute("UPDATE foo SET a = 10 WHERE id = 1")
    db1.commit()

    db1_db_version = db1.execute("SELECT crsql_db_version()").fetchone()[0]
    assert (db1_db_version == 3)


def test_ts_is_inserted():
    def create_db():
        db = connect(":memory:")
        db.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY NOT NULL, a, b)")
        db.execute("SELECT crsql_as_crr('foo');")
        db.commit()
        return db

    def get_site_id(db):
        return db.execute("SELECT crsql_site_id()").fetchone()[0]

    db1 = create_db()
    db2 = create_db()

    db1_site_id = get_site_id(db1)
    db2_site_id = get_site_id(db2)

    # use max u64
    db1.execute("SELECT crsql_set_ts('18446744073709551615');")
    db1.execute("INSERT INTO foo (id, a, b) VALUES (1, 2, 3)")
    db1.commit()

    # ts should be zero if it isn't set in a transaction
    db1.execute("INSERT INTO foo (id, a, b) VALUES (2, 5, 6)")
    db1.commit()

    sync_left_to_right(db1, db2, 0)

    db1_changes = db1.execute("SELECT * FROM crsql_changes").fetchall()

    assert (db1_changes == [('foo', b'\x01\t\x01', 'a', 2, 1, 1, db1_site_id, 1, 0, '18446744073709551615'),
                    ('foo', b'\x01\t\x01', 'b', 3, 1, 1, db1_site_id, 1, 1, '18446744073709551615'),
                    ('foo', b'\x01\t\x02', 'a', 5, 1, 2, db1_site_id, 1, 0, '0'),
                    ('foo', b'\x01\t\x02', 'b', 6, 1, 2, db1_site_id, 1, 1, '0')])

    db2_changes = db2.execute("SELECT * FROM crsql_changes").fetchall()
    assert (db2_changes == db1_changes)
