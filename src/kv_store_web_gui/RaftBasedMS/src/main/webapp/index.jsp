<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
    <head>
        <title>Raft-Based Management System</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-giJF6kkoqNQ00vy+HMDP7azOuL0xtbfIcaT9wjKHr8RbDVddVHyTfAAsrekwKmP1" crossorigin="anonymous">
        <link href="resources/css/style.css" rel="stylesheet">
    </head>
    <body>
        <%@include file="WEB-INF/commons/header.jsp"%>
        <main>
            <section>
                <div class="container mx-auto mt-2" style="width: 750px">
                    <h3 class="mt-2">Architecture</h3>
                    <img src="resources/images/raft_architecture.PNG" class="img-fluid mt-2" alt="Project architecture">
                    <h3 class="mt-4">What is possible?</h3>
                    <p>The possible operations on the distributed KV store are the following:
                    <ul>
                        <li><b>V get(K key)</b>: returns the value associated with the key</li>
                        <li><b>Map{K,V} getAll()</b>: returns all key-value pairs</li>
                        <li><b>void set(K key, V value)</b>: sets the given value to the provided key</li>
                        <li><b>void delete(K key)</b>: deletes the item with the given key</li>
                        <li><b>void deleteAll()</b>: deletes all the keys</li>
                    </ul>
                    </p>
                    <h3 class="mt-4">How?</h3>
                    <p class="">In order to manage data, click on <b class="text-uppercase">Manage data</b> tab</p>
                </div>
            </section>
        </main>
        <%@include file="WEB-INF/commons/footer.jsp"%>
    </body>
</html>