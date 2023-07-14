<%= sql("SELECT * FROM tests;").to_json(#{pretty: true}) %>
<%= sql("SELECT * FROM tests;").to_json(#{pretty: true}) %>
<%= sql("SELECT id AS testid2, foo AS testfoo2 FROM tests;").to_json(#{pretty: true}) %>