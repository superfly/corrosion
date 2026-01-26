from crsql_correctness import connect, close, min_db_v
from pprint import pprint
import pytest
import time

def test_commit_alter():
  c = connect(":memory:")
  c.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY NOT NULL, title TEXT)")
  c.execute("SELECT crsql_as_crr('foo')")
  c.commit()

  c.execute("INSERT INTO foo (id, title) VALUES (1, 'new title')")
  c.commit()

  start_time = time.time()
  c.execute("SELECT crsql_begin_alter('foo')")
  c.execute("ALTER TABLE foo ADD COLUMN owner TEXT")
  c.execute("SELECT crsql_commit_alter('main', 'foo', 1)")
  end_time = time.time()
  print(f"non-destructive alter time: {end_time - start_time}")
  
  prev_version = c.execute("SELECT crsql_db_version()").fetchone()[0]

  c.execute("INSERT INTO foo (id, title) VALUES (2, 'another title')")
  c.commit()
  current_version = c.execute("SELECT crsql_db_version()").fetchone()[0]
  assert current_version == prev_version + 1
  # assert new changes in crsql_changes
  changes = c.execute("SELECT pk, cid, val FROM crsql_changes where db_version = {} ORDER BY cid".format(current_version)).fetchall()
  assert (changes == [(b'\x01\t\x02', 'owner', None),
                      (b'\x01\t\x02', 'title', 'another title')])
  table_changes = c.execute("SELECT * FROM foo where id = 2").fetchall()
  assert(table_changes == [(2, 'another title', None)])

  c.execute("UPDATE foo SET title = 'update title', owner = 'new owner' where id = 1")
  c.commit()
  next_version = c.execute("SELECT crsql_db_version()").fetchone()[0]
  assert(next_version, current_version + 1)
  next_change = c.execute("SELECT pk, cid, val FROM crsql_changes where db_version = {} ORDER BY cid".format(next_version)).fetchall()
  print("next version: {next_version}")
  assert (next_change == [(b'\x01\t\x01', 'owner', 'new owner'),
                          (b'\x01\t\x01', 'title', 'update title')])

  c.execute("INSERT INTO foo (id, title, owner) VALUES (3, 'third title', 'third owner')")
  c.commit()
  next_version = c.execute("SELECT crsql_db_version()").fetchone()[0]
  next_change = c.execute("SELECT pk, cid, val FROM crsql_changes where db_version = {} ORDER BY cid".format(next_version)).fetchall()
  assert (next_change == [(b'\x01\t\x03', 'owner', 'third owner'),
                          (b'\x01\t\x03', 'title', 'third title')])

  c.execute("DELETE FROM foo where id = 3")
  c.commit()
  next_version = c.execute("SELECT crsql_db_version()").fetchone()[0]
  next_change = c.execute("SELECT pk, cid FROM crsql_changes where db_version = {} ORDER BY cid".format(next_version)).fetchall()
  assert (next_change == [(b'\x01\t\x03', '-1')])
 
  None