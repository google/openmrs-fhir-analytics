
# Fhir-data-pipes

An open-source pipeline to transform data from a FHIR server (like HAPI, GCP FHIR store, or even OpenMRS) using the FHIR format into a data warehouse based on Apache Parquet files, or another FHIR server.

## TL;DR

```bash
$ helm repo add google-fhir-data-pipes https://google.github.io/fhir-data-pipes/
$ helm install fhir-data-pipes google-fhir-data-pipes/fhir-data-pipes
```

## Introduction

This chart bootstraps  [fhir-data-pipes](https://github.com/google/fhir-data-pipes) deployment on a [Kubernetes](http://kubernetes.io) cluster using the [Helm](https://helm.sh) package manager.

## Prerequisites

- Kubernetes 1.12+
- Helm 3.1.0

## Configuration

The following table lists the configurable parameters of the Fhir-data-pipes chart and their default values.

| Parameter                                             | Description | Default                                                                                               |
|-------------------------------------------------------|-------------|-------------------------------------------------------------------------------------------------------|
| `replicaCount`                                        |             | `1`                                                                                                   |
| `image.repository`                                    |             | `"google/fhir-data-pipes"`                                                                            |
| `image.pullPolicy`                                    |             | `"IfNotPresent"`                                                                                      |
| `image.tag`                                           |             | `"master"`                                                                                            |
| `imagePullSecrets`                                    |             | `[]`                                                                                                  |
| `nameOverride`                                        |             | `""`                                                                                                  |
| `fullnameOverride`                                    |             | `""`                                                                                                  |
| `serviceAccount.create`                               |             | `true`                                                                                                |
| `serviceAccount.annotations`                          |             | `{}`                                                                                                  |
| `serviceAccount.name`                                 |             | `""`                                                                                                  |
| `podAnnotations`                                      |             | `{}`                                                                                                  |
| `podSecurityContext`                                  |             | `{}`                                                                                                  |
| `securityContext`                                     |             | `{}`                                                                                                  |
| `service.type`                                        |             | `"ClusterIP"`                                                                                         |
| `service.port`                                        |             | `8080`                                                                                                |
| `service.extraPorts`                                  |             | `null`                                                                                                |
| `service.headless.type`                               |             | `"ClusterIP"`                                                                                         |
| `ingress.enabled`                                     |             | `false`                                                                                               |
| `ingress.className`                                   |             | `""`                                                                                                  |
| `ingress.annotations`                                 |             | `{}`                                                                                                  |
| `ingress.hosts`                                       |             | `[{"host": "fhir-data-pipes.local", "paths": [{"path": "/", "pathType": "ImplementationSpecific"}]}]` |
| `ingress.tls`                                         |             | `[]`                                                                                                  |
| `ingress.extraRules`                                  |             | `null`                                                                                                |
| `resources`                                           |             | `null`                                                                                                |
| `autoscaling.enabled`                                 |             | `false`                                                                                               |
| `autoscaling.minReplicas`                             |             | `1`                                                                                                   |
| `autoscaling.maxReplicas`                             |             | `100`                                                                                                 |
| `autoscaling.targetCPUUtilizationPercentage`          |             | `80`                                                                                                  |
| `nodeSelector`                                        |             | `{}`                                                                                                  |
| `tolerations`                                         |             | `[]`                                                                                                  |
| `affinity`                                            |             | `{}`                                                                                                  |
| `recreatePodsWhenConfigMapChange`                     |             | `true`                                                                                                |
| `livenessProbe.httpGet.path`                          |             | `"/"`                                                                                                 |
| `livenessProbe.httpGet.port`                          |             | `"http"`                                                                                              |
| `readinessProbe.httpGet.path`                         |             | `"/"`                                                                                                 |
| `readinessProbe.httpGet.port`                         |             | `"http"`                                                                                              |
| `flink.execution.attached`                            |             | `false`                                                                                               |
| `hapi.postgres.databaseService`                       |             | `"postgresql"`                                                                                        |
| `hapi.postgres.databaseHostName`                      |             | `""`                                                                                                  |
| `hapi.postgres.databasePort`                          |             | `"5432"`                                                                                              |
| `hapi.postgres.databaseUser`                          |             | `""`                                                                                                  |
| `hapi.postgres.databasePassword`                      |             | `""`                                                                                                  |
| `hapi.postgres.databaseName`                          |             | `""`                                                                                                  |
| `thriftserver.hive.databaseService`                   |             | `"hive2"`                                                                                             |
| `thriftserver.hive.databaseHostName`                  |             | `"spark-thriftserver"`                                                                                |
| `thriftserver.hive.databasePort`                      |             | `"10000"`                                                                                             |
| `thriftserver.hive.databaseUser`                      |             | `"hive"`                                                                                              |
| `thriftserver.hive.databasePassword`                  |             | `""`                                                                                                  |
| `thriftserver.hive.databaseName`                      |             | `"default"`                                                                                           |
| `applicationConfig.fhirdata.fhirServerUrl`            |             | `""`                                                                                                  |
| `applicationConfig.fhirdata.dbConfig`                 |             | `"/app/config/hapi-postgres-config.json"`                                                             |
| `applicationConfig.fhirdata.dwhRootPrefix`            |             | `"/dwh/controller_DWH"`                                                                               |
| `applicationConfig.fhirdata.thriftserverHiveConfig`   |             | `"/app/config/thrifter-hive-config.json"`                                                             |
| `applicationConfig.fhirdata.incrementalSchedule`      |             | `"* * * * * *"`                                                                                       |
| `applicationConfig.fhirdata.resourceList`             |             | `"PatientEncounterObservation"`                                                                       |
| `applicationConfig.fhirdata.maxWorkers`               |             | `"10"`                                                                                                |
| `applicationConfig.fhirdata.createHiveResourceTables` |             | `"true"`                                                                                              |
| `applicationConfig.fhirdata.hiveJdbcDriver`           |             | `"org.apache.hive.jdbc.HiveDriver"`                                                                   |
| `initContainers`                                      |             | `null`                                                                                                |
| `sidecars`                                            |             | `null`                                                                                                |
| `extraVolumes`                                        |             | `null`                                                                                                |
| `extraVolumeMounts`                                   |             | `null`                                                                                                |
| `extraConfigMaps`                                     |             | `null`                                                                                                |
| `env`                                                 |             | `null`                                                                                                |
| `pdb.enabled`                                         |             | `false`                                                                                               |
| `pdb.minAvailable`                                    |             | `""`                                                                                                  |
| `pdb.maxUnavailable`                                  |             | `1`                                                                                                   |
| `vpa.enabled`                                         |             | `false`                                                                                               |
| `vpa.updatePolicy.updateMode`                         |             | `"Off"`                                                                                               |
| `vpa.resourcePolicy`                                  |             | `{}`                                                                                                  |
| `pvc.enabled`                                         |             | `true`                                                                                                |
| `pvc.volumeMode`                                      |             | `"Filesystem"`                                                                                        |
| `pvc.storageClassName`                                |             | `null`                                                                                                |
| `pvc.resources.requests.storage`                      |             | `"20Gi"`                                                                                              |
| `pvc.accessModes`                                     |             | `["ReadWriteOnce"]`                                                                                   |
| `pvc.selector`                                        |             | `{}`                                                                                                  |
| `hiveSiteConfig`                                      |             | `null`                                                                                                |


## Spark SQL (Thrift Server) as Sidecar
The chart provides the necessary configuration to set up additional containers on the StatefulSet. One such container is the spark thrift server. Below is how one can set it up.
````yaml
---
sidecars:
  - name: spark-thrift-server
    image: docker.io/bitnami/spark:3.3.2-debian-11-r12
    imagePullPolicy: IfNotPresent
    ports:
      - name: hive
        containerPort: 10000
      - name: hive-webui
        containerPort: 4040
    args:
      - "/bin/sh"
      - "-c"
      - "sbin/start-thriftserver.sh"
      - "--master spark://spark-master-svc:7077"
    env:
      - name: "HIVE_SERVER2_THRIFT_PORT"
        value: "10000"
    resources:
      limits:
        memory: 1024Mi
      requests:
        cpu: 250m
        memory: 256Mi
    volumeMounts:
      - name: dwh-dir
        mountPath: /dwh
      - name: hive-site-xml
        mountPath: /opt/bitnami/spark/conf/hive-site.xml
        subPath: hive-site-xml

# Creates a volume for parquet files.
pvc:
  enabled: true
  volumeMode: Filesystem
  storageClassName:
  resources:
    requests:
      storage: 20Gi
  accessModes:
    - ReadWriteOnce
  selector: {}

#  One can expose the thrift server port using tcp services e.g. for nginx(https://github.com/kubernetes/ingress-nginx/blob/main/docs/user-guide/exposing-tcp-udp-services.md) 
service:
  type: ClusterIP
  port: 8080
  extraPorts:
    - port: 4040
      targetPort: hive-webui
      protocol: TCP
      name: hive-webui
    - port: 10000
      targetPort: hive
      protocol: TCP
      name: hive

# For LDAP ensure that user has the attribute `uid` on its DN.
# As of now the hive jdbc credentials are not parsed on the pipeline code, as a workaround append the credentials on the `databaseName` credentials as follows:
#thriftserver:
#  hive:
#    databaseService: "hive2"
#    databaseHostName: "fhir-data-pipes"
#    databasePort: "10000"
#    databaseUser: "user" (not used as of now)
#    databasePassword: "password" (not used as of now)
#    databaseName: "default;user=<userid>;password=<password>"

hiveSiteConfig: |
  <?xml version="1.0"?>
  <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

  <configuration>
    <property>
     <name>hive.server2.authentication</name>
     <value>LDAP</value>
     </property>
    <property>
      <name>hive.server2.authentication.ldap.url</name>
      <value>ldap://openldap.default.svc.cluster.local:389</value>
    </property>
    <property>
      <name>hive.server2.authentication.ldap.baseDN</name>
      <value>ou=users,dc=ldapadmin,dc=labs,dc=example,dc=org</value>
    </property>
  </configuration>
````
