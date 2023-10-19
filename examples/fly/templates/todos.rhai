/* Template for todo checklist */

<% for todo in sql("SELECT title, completed_at FROM todos") { %>
[<% if todo.completed_at.is_null() { %> <% } else { %>X<% } %>] <%= todo.title %>
<% } %>