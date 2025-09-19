helm upgrade postgres-topology-operator . -n postgres-operator --set operator.enable=true --set operator.imagePullPolicy=IfNotPresent --set operator.image=digizuite.azurecr.io/digizuite/postgres-topology-operator:task-self-signed-certificates --install --create-namespace

