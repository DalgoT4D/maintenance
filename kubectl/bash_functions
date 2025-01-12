abpods() {
	kubectl get pods -n airbyte | grep -v -E "airbyte-airbyte-bootloader|airbyte-connector-builder-server-|airbyte-cron-|airbyte-pod-sweeper-pod-sweeper-|airbyte-server-|airbyte-temporal-|airbyte-webapp-|airbyte-worker-|airbyte-workload-api-server-|airbyte-workload-launcher-"
}

ablogs() {
	kubectl logs $1 -n airbyte
}

abdesc() {
	kubectl describe pod $1 -n airbyte
}

pf() {
	podname=`kubectl get pods -n airbyte | grep airbyte-webapp- | awk '{print $1}'`
	echo "port forwarding $podname"
	kubectl port-forward $podname -n airbyte 8080:8080 -n airbyte
}
