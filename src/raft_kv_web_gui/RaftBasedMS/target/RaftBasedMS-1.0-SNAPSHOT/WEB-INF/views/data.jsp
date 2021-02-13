<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<html>
  <head>
    <title>Raft-Based Management System</title>
    <script src="https://code.jquery.com/jquery-3.5.1.js"></script>
    <script src="https://cdn.datatables.net/1.10.23/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/1.10.23/js/dataTables.bootstrap5.min.js"></script>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-giJF6kkoqNQ00vy+HMDP7azOuL0xtbfIcaT9wjKHr8RbDVddVHyTfAAsrekwKmP1" crossorigin="anonymous">
    <link href="https://cdn.datatables.net/1.10.23/css/dataTables.bootstrap5.min.css" rel="stylesheet" integrity="sha384-giJF6kkoqNQ00vy+HMDP7azOuL0xtbfIcaT9wjKHr8RbDVddVHyTfAAsrekwKmP1" crossorigin="anonymous">
    <link href="resources/css/style.css" rel="stylesheet">
  </head>
  <body>
      <%@include file="../commons/header.jsp"%>
      <main>
        <section>
          <div class="container mx-auto" style="width: 750px">
            <table id="datatable" class="table table-striped table-bordered dt-responsive nowrap" style="width:100%">
              <thead>
                <tr>
                  <th>Key</th>
                  <th>Value</th>
                  <th>Delete</th>
                </tr>
              </thead>
              <tbody>${kvData}</tbody>
            </table>
            <c:if test="${not empty deleteOpResult}">
                <c:choose>
                  <c:when test="${deleteOpResult == true}">
                      <div class="alert alert-success" role="alert">
                          Record deleted successfully
                      </div>
                  </c:when>
                  <c:when test="${deleteOpResult == false}">
                      <div class="alert alert-danger" role="alert">
                          Something goes wrong, please try again!
                      </div>
                  </c:when>
                </c:choose>
            </c:if>
          </div>
        </section>
        <section>
        </section>
      <section class="mt-2">
          <div class="container mx-auto" style="width: 750px">
              <div class="row mb-2">
                <div class="col-6">
                    <div class="input-group">
                        <div class="input-group-prepend">
                            <span class="input-group-text">Flush the raft KV store</span>
                        </div>
                        <div class="input-group-append">
                            <form action="data" method="post">
                                <button name="deleteAll" class="btn btn-outline-danger" type="submit">Delete All</button>
                            </form>
                        </div>
                    </div>
                    <c:if test="${not empty deleteAllOpResult}">
                        <c:choose>
                            <c:when test="${deleteAllOpResult == true}">
                                <div class="alert alert-success" role="alert">
                                    All records has been deleted successfully
                                </div>
                            </c:when>
                            <c:when test="${deleteAllOpResult == false}">
                                <div class="alert alert-danger" role="alert">
                                    Something goes wrong, please try again!
                                </div>
                            </c:when>
                        </c:choose>
                    </c:if>
                </div>
                <div class="col-1">
                    <span>Set a value</span>
                </div>
                <div class="col-5">
                    <form action="data" method="post">
                        <input name="key" value="" placeholder="Insert the key...">
                        <input name="value" value="" placeholder="Insert the value...">
                        <button name="set" class="btn btn-outline-success" type="submit">Set</button>
                    </form>
                    <c:if test="${not empty setOpResult}">
                        <c:choose>
                            <c:when test="${setOpResult == true}">
                                <div class="alert alert-success" role="alert">
                                    New record inserted successfully
                                </div>
                            </c:when>
                            <c:when test="${setOpResult == false}">
                                <div class="alert alert-danger" role="alert">
                                    Something goes wrong, please try again!
                                </div>
                            </c:when>
                        </c:choose>
                    </c:if>
                </div>
                <div class="col-8">
                    <form action="data" method="get">
                        <label for="key">Get a value</label>
                        <input id="key" name="key" value="" placeholder="Insert the key...">
                        <input name="get" type="submit" class="btn btn-outline-primary" value="Get">
                    </form>
                    <c:if test="${not empty requestedValue and requestedValue != ''}">
                        <div class="alert alert-success" role="alert">
                            The value associated with the given key is: ${requestedValue}
                        </div>
                    </c:if>
                </div>
              </div>
          </div>
      </section>
      </main>
      <%@include file="../commons/footer.jsp"%>
  </body>
<script>
  $(document).ready(function() {
    $('#datatable').DataTable({
      "lengthMenu": [4, 7]
    });
  } );
</script>
</html>