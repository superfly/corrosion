import pathlib
import sqlite3
import pytest
from crsql_correctness import connect, close, min_db_v, get_site_id, sync_left_to_right

# c1


def test_min_on_init():
    c = connect(":memory:")
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v

# c2


def test_increments_on_modification():
    c = connect(":memory:")
    c.execute("create table foo (id primary key not null, a)")
    c.execute("select crsql_as_crr('foo')")
    c.execute("insert into foo values (1, 2)")
    c.execute("commit")
    # +2 since create table statements bump version too
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 1
    c.execute("update foo set a = 3 where id = 1")
    c.execute("commit")
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 2
    c.execute("delete from foo where id = 1")
    c.execute("commit")
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 3
    close(c)

def test_db_version_restored_from_disk():
    dbfile = "./dbversion_c3.db"
    pathlib.Path(dbfile).unlink(missing_ok=True)
    c = connect(dbfile)

    # C3
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v

    # close and re-open to check that we work with empty clock tables
    c.execute("create table foo (id primary key not null, a)")
    c.execute("select crsql_as_crr('foo')")
    c.close()
    c = connect(dbfile)
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v

    # insert so we get a clock entry
    c.execute("insert into foo values (1, 2)")
    c.commit()
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 1

    # insert so we get a clock entry
    c.execute("insert into foo values (2, 3)")
    c.commit()
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == 2

    # Close and reopen to check that version was persisted and re-initialized correctly
    close(c)
    c = connect('file:dbversion_c3.db?mode=ro', uri=True)
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == 2
    close(c)

    c = connect(dbfile)
    # create a new db and sync with it
    c2 = connect(":memory:")
    c2.execute("CREATE TABLE foo (id primary key not null, a);")
    c2.execute("SELECT crsql_as_crr('foo');")

    sync_left_to_right(c, c2, 0)
    assert c2.execute("SELECT crsql_db_version()").fetchone()[0] == 0

    # create changes that would override rows in db1
    c2.execute("UPDATE foo SET a = 5")
    c2.commit()
    assert c2.execute("SELECT crsql_db_version()").fetchone()[0] == 1

    sync_left_to_right(c2, c, 0)
    assert c2.execute("SELECT crsql_db_version()").fetchone()[0] == 1
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == 2

    # Close and reopen to check that version was persisted and re-initialized correctly
    close(c)
    c = connect('file:dbversion_c3.db?mode=ro', uri=True)
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == 2

    close(c)

# c4


def test_each_tx_gets_a_version():
    c = connect(":memory:")

    c.execute("create table foo (id primary key not null, a)")
    c.execute("select crsql_as_crr('foo')")
    c.execute("insert into foo values (1, 2)")
    c.execute("insert into foo values (2, 2)")
    c.commit()
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 1

    c.execute("insert into foo values (3, 2)")
    c.execute("insert into foo values (4, 2)")
    c.commit()
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 2

    close(c)


def test_rollback_does_not_move_db_version():
    c = connect(":memory:")

    c.execute("create table foo (id primary key not null, a)")
    c.execute("select crsql_as_crr('foo')")

    c.execute("insert into foo values (1, 2)")
    c.execute("insert into foo values (2, 2)")
    c.rollback()
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v

    c.execute("insert into foo values (3, 2)")
    c.execute("insert into foo values (4, 2)")
    c.rollback()
    assert c.execute("SELECT crsql_db_version()").fetchone()[
        0] == min_db_v

    c.execute("insert into foo values (1, 2)")
    c.execute("insert into foo values (2, 2)")
    c.commit()
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 1

    c.execute("insert into foo values (3, 2)")
    c.execute("insert into foo values (4, 2)")
    c.commit()
    assert c.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 2

    close(c)

def test_overwriting_keeps_track_of_true_db_version():
    def create_db():
        db1 = connect(":memory:")
        db1.execute("CREATE TABLE foo (a PRIMARY KEY NOT NULL, b DEFAULT 0);")
        db1.execute("SELECT crsql_as_crr('foo');")
        db1.commit()
        return db1


    db1 = create_db()
    db2 = create_db()

    db1_site_id = get_site_id(db1)
    db2_site_id = get_site_id(db2)


    db1.execute("INSERT INTO foo (a) VALUES (1);")
    db1.commit() # db_version = 1
    assert db1.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 1

    sync_left_to_right(db1, db2, 0)

    db1.execute("UPDATE foo SET b = 1;")
    db1.commit() # db_version = 2
    assert db1.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 2

    sync_left_to_right(db1, db2, 0)

    db2.execute("UPDATE foo SET b = 2;")
    db2.commit()

    sync_left_to_right(db2, db1, 0)

    assert db2.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 1
    assert db2.execute("SELECT db_version from crsql_db_versions where site_id = ?", (bytes(db1_site_id),)).fetchone()[0] == min_db_v + 2

    assert db1.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 2
    assert db1.execute("SELECT db_version from crsql_db_versions where site_id = ?", (bytes(db2_site_id),)).fetchone()[0] == min_db_v + 1

    changes = db1.execute("SELECT * FROM crsql_changes").fetchall()
    assert (changes == [('foo', b'\x01\t\x01', 'b', 2, 3, 1, db2_site_id, 1, 0, '0')])

    db1.execute("UPDATE foo SET b = 3;")
    db1.commit() # db_version = 3

    sync_left_to_right(db1, db2, 0)

    changes = db1.execute("SELECT * FROM crsql_changes").fetchall()

    assert (changes == [('foo', b'\x01\t\x01', 'b', 3, 4, 3, db1_site_id, 1, 0, '0')])

    db_versions_1 = db1.execute("SELECT * FROM crsql_db_versions").fetchall()
    db_versions_2 = db2.execute("SELECT * FROM crsql_db_versions").fetchall()

    assert db_versions_1 == [(db1_site_id, 3), (db2_site_id, 1)]

    assert db_versions_1 == db_versions_2

    ## test that db_version is updated even when a received change does not win.
    db1.execute("UPDATE foo SET b = 4;")
    db1.commit()
    db1.execute("UPDATE foo SET b = 5;")
    db1.commit()

    db2.execute("UPDATE foo SET b = 6;") # this change will lose at db1
    db2.commit()
    assert db2.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 2


    sync_left_to_right(db2, db1, 0)
    assert db1.execute("SELECT crsql_db_version()").fetchone()[0] == min_db_v + 5
    assert db1.execute('SELECT db_version from crsql_db_versions where site_id = ?', (bytes(db2_site_id),)).fetchone()[0] == min_db_v + 2

    sync_left_to_right(db1, db2, 0)
    db_versions_1 = db1.execute("SELECT * FROM crsql_db_versions").fetchall()
    db_versions_2 = db2.execute("SELECT * FROM crsql_db_versions").fetchall()

    assert db_versions_1 == [(db1_site_id, 5), (db2_site_id, 2)]

    assert db_versions_1 == db_versions_2

    close(db1)
    close(db2)

def test_site_id_ordinal_is_set_and_updated():
    c1 = connect(":memory:")
    c2 = connect(":memory:")

    other_site_id = get_site_id(c2)
    c1_site_id = get_site_id(c1)
    # first insert, assert db_version and ordinal is set
    c1.execute("SELECT crsql_set_db_version(?, ?)", (bytes(other_site_id), 3))
    c1.commit()
    assert c1.execute("SELECT db_version from crsql_db_versions where site_id = ?", (bytes(other_site_id),)).fetchone()[0] == 3
    assert c1.execute("SELECT count(*) from crsql_site_id").fetchone()[0] == 2
    assert c1.execute("SELECT ordinal from crsql_site_id where site_id = ?", (bytes(other_site_id),)).fetchone()[0] == 1

    c1.execute("SELECT crsql_set_db_version(?, ?)", (bytes(other_site_id), 5))
    assert c1.execute("SELECT db_version from crsql_db_versions where site_id = ?", (bytes(other_site_id),)).fetchone()[0] == 5

    # smaller db_version should not get set`x`
    c1.execute("SELECT crsql_set_db_version(?, ?)", (bytes(other_site_id), 2))
    assert c1.execute("SELECT db_version from crsql_db_versions where site_id = ?", (bytes(other_site_id),)).fetchone()[0] == 5

    # setting our own site_id should error
    with pytest.raises(sqlite3.Error):
        c1.execute("SELECT crsql_set_db_version(?, ?)", (bytes(c1_site_id), 6))

    close(c1)
    close(c2)
