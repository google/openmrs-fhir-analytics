#!/usr/bin/env bash

# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Example usage:
#   ./controller_spark_sql_validation.sh ./ dwh
#   ./controller_spark_sql_validation.sh ./ dwh --use_docker_network

set -e

#################################################
# Prints the usage
#################################################
function usage() {
  echo "This script validates if number of resources captured through"
  echo "parquet files match what is stored in the source FHIR server"
  echo
  echo " usage: ./controller_spark_sql_validation.sh  HOME_DIR  PARQUET_SUBDIR  [OPTIONS] "
  echo "    HOME_DIR          Path where e2e-tests/controller-spark directory is. Directory MUST"
  echo "                      contain the parquet tools jar as well as subdirectory"
  echo "                      of parquet file output"
  echo "    PARQUET_SUBDIR    Subdirectory name under HOME_DIR containing"
  echo "                      parquet files  "
  echo
  echo " Options:  "
  echo "     --use_docker_network     Flag to specify whether to use docker"
  echo "                              or host network URLs"
}

#################################################
# Makes sure args passed are correct
#################################################
function validate_args() {
  if [[ $# -lt 2 || $# -gt 5  ]]; then
    echo "Invalid number of args passed."
    usage
    exit 1
  fi

  echo "Checking if the Parquet-tools JAR exists..."
  if [[ -n $(find "${1}" -name parquet-tools*.jar) ]]; then
    echo "Parquet-tools JAR exists in ${1}"
  else
    echo "Parquet-tools JAR not found in ${1}"
    usage
    exit 1
  fi
}

#################################################
# Function that prints messages
# Arguments:
#   anything that needs printing
#################################################
function print_message() {
  local print_prefix=""
  if [[ "${DWH_TYPE}" == "PARQUET" ]]
  then
    print_prefix="E2E TEST FOR CONTROLLER PARQUET BASED DEPLOYMENT:"
  else
    print_prefix="E2E TEST FOR CONTROLLER FHIR SERVER TO FHIR SERVER SYNC:"
  fi
  echo "${print_prefix} $*"
}

#################################################
# Function that defines the global vars
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   SOURCE_FHIR_SERVER_URL
#   SINK_FHIR_SERVER_URL
#   PIPELINE_CONTROLLER_URL
#   THRIFTSERVER_URL
# Arguments:
#   Path where e2e-tests/controller-spark directory is. Directory contains parquet tools jar as
#      well as subdirectory of parquet file output
#   Subdirectory name under HOME_DIR containing parquet files.
#      Example: dwh
#   Optional: Flag to specify whether to use docker or host network URLs.
#################################################
function setup() {
  HOME_PATH=$1
  PARQUET_SUBDIR=$2
  DWH_TYPE=$4
  SOURCE_FHIR_SERVER_URL='http://localhost:8091'
  SINK_FHIR_SERVER_URL='http://localhost:8098'
  PIPELINE_CONTROLLER_URL='http://localhost:8090'
  THRIFTSERVER_URL='localhost:10001'
  if [[ $3 = "--use_docker_network" ]]; then
    SOURCE_FHIR_SERVER_URL='http://hapi-server:8080'
    SINK_FHIR_SERVER_URL='http://sink-server-controller:8080'
    PIPELINE_CONTROLLER_URL='http://pipeline-controller:8080'
    THRIFTSERVER_URL='spark:10000'
  fi
}

#######################################################################
# This function queries fhir server and writes results to json files.
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
# Arguments:
#  server_url: url of the source fhir server.
#  patient_json_file : file to write Patient results
#  encounter_json_file : file to write Encounter results
#  obs_json_file : file to write Observation results
#######################################################################
function query_fhir_server(){
  local query_param="?_summary=count"
  local server_url=$1
  local patient_json_file=$2
  local encounter_json_file=$3
  local obs_json_file=$4

  print_message "Finding number of patients, encounters and obs in FHIR server ${server_url}"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --header 'Cache-Control: no-cache' --max-time 20 "${server_url}/fhir/Patient${query_param}"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --header 'Cache-Control: no-cache' --max-time 20 \
  "${server_url}/fhir/Patient${query_param}" 2>/dev/null \
  >"${HOME_PATH}/${PARQUET_SUBDIR}/${patient_json_file}"

  print_message "Write Patients into File"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --header 'Cache-Control: no-cache' --max-time 20 \
  "${server_url}/fhir/Encounter${query_param}" 2>/dev/null \
  >"${HOME_PATH}/${PARQUET_SUBDIR}/${encounter_json_file}"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --header 'Cache-Control: no-cache' --max-time 20 \
  "${server_url}/fhir/Observation${query_param}" 2>/dev/null\
  >"${HOME_PATH}/${PARQUET_SUBDIR}/${obs_json_file}"

  print_message "Write Observation into File"
}

#################################################
# Function to count resources in source fhir server
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   SOURCE_FHIR_SERVER_URL
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
#################################################
function fhir_source_query() {

  query_fhir_server "${SOURCE_FHIR_SERVER_URL}"  "patients.json" "encounters.json" "obs.json"
  print_message "Before count"
  TOTAL_TEST_PATIENTS=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/patients.json")
  print_message "Total FHIR source test patients ---> ${TOTAL_TEST_PATIENTS}"

  TOTAL_TEST_ENCOUNTERS=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/encounters.json")
  print_message "Total FHIR source test encounters ---> ${TOTAL_TEST_ENCOUNTERS}"

  TOTAL_TEST_OBS=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/obs.json")
  print_message "Total FHIR source test obs ---> ${TOTAL_TEST_OBS}"
}

#######################################################################
# Function to send command to pipeline controller to start pipeline.
# Globals:
#   PIPELINE_CONTROLLER_URL
# Arguments:
#   runMode: flag to indicate whether to start full or incremental or recreate
#     runs; should be one of "FULL", "INCREMENTAL", "VIEWS".
#######################################################################
function run_pipeline() {
  local runMode=$1
  curl --location --request POST "${PIPELINE_CONTROLLER_URL}/run?runMode=${runMode}" \
  --connect-timeout 5 \
  --header 'Content-Type: application/json' \
  --header 'Accept: */*' -v
}

function wait_for_completion() {
  local runtime="15 minute"
  local end_time=$(date -ud "$runtime" +%s)

  while [[ $(date -u +%s) -le $end_time ]]
  do
    local pipeline_status=$(curl --location --request GET "${PIPELINE_CONTROLLER_URL}/status?" \
    --connect-timeout 5 \
    --header 'Content-Type: application/json' \
    --header 'Accept: */*' -v \
    | jq -r '.pipelineStatus')

    if [[ "${pipeline_status}" == "RUNNING" ]]
    then
      sleep 5
    else
      break
    fi
  done
}

#######################################################################
# Function to check periodically parquet files in the given directory
# and verify the number of created resources against the number got from
# hapi server.
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
# Arguments:
#   isIncremental: flag to differentiate behavior between incremental and batch runs.
#######################################################################
function check_parquet() {
  local isIncremental=$1
  local output="${HOME_PATH}/${PARQUET_SUBDIR}"
  TOTAL_VIEW_PATIENTS=106

  if [[ "${isIncremental}" == "true" ]]
  then
    # In case of incremental run, we will have two directories
    # assuming batch run was executed before this.
    TOTAL_TEST_PATIENTS=$((2*TOTAL_TEST_PATIENTS + 1))
    TOTAL_VIEW_PATIENTS=213
    TOTAL_TEST_ENCOUNTERS=$((2*TOTAL_TEST_ENCOUNTERS))
    TOTAL_TEST_OBS=$((2*TOTAL_TEST_OBS))
  fi

  # check whether output directory has received parquet files.
  if [[ "$(ls -A $output)" ]]
  then
    local total_patients=$(java -Xms16g -Xmx16g -jar ./parquet-tools-1.11.1.jar rowcount \
    "${output}/*/Patient/" | awk '{print $3}')
    local total_encounters=$(java -Xms16g -Xmx16g -jar ./parquet-tools-1.11.1.jar rowcount \
    "${output}/*/Encounter/" | awk '{print $3}')
    local total_observations=$(java -Xms16g -Xmx16g -jar ./parquet-tools-1.11.1.jar rowcount \
    "${output}/*/Observation/" | awk '{print $3}')

    local total_patient_flat=$(java -Xms16g -Xmx16g -jar ./parquet-tools-1.11.1.jar rowcount \
    "${output}/*/patient_flat/" | awk '{print $3}')
    local total_encounter_flat=$(java -Xms16g -Xmx16g -jar ./parquet-tools-1.11.1.jar rowcount \
    "${output}/*/encounter_flat/" | awk '{print $3}')
    local total_obs_flat=$(java -Xms16g -Xmx16g -jar ./parquet-tools-1.11.1.jar rowcount \
     "${output}/*/observation_flat/" | awk '{print $3}')

    print_message "Total patients: $total_patients"
    print_message "Total encounters: $total_encounters"
    print_message "Total observations: $total_observations"

    print_message "Total patient flat rows: ${total_patient_flat}"
    print_message "Total encounter flat rows: ${total_encounter_flat}"
    print_message "Total observation flat rows: ${total_obs_flat}"

    if (( total_patients == TOTAL_TEST_PATIENTS \
            && total_encounters == TOTAL_TEST_ENCOUNTERS && \
            total_observations == TOTAL_TEST_OBS \
            && total_obs_flat == TOTAL_TEST_OBS && \
            total_patient_flat == TOTAL_VIEW_PATIENTS && \
            total_encounter_flat == TOTAL_TEST_ENCOUNTERS )); then
            print_message "Pipeline transformation successfully completed."
    else
            print_message "Mismatch in count of records"
            print_message "Actual total patients: $total_patients, expected total: $TOTAL_TEST_PATIENTS"
            print_message "Actual total encounters: $total_encounters, expected total: $TOTAL_TEST_ENCOUNTERS"
            print_message "Total observations: $total_observations, expected total: $TOTAL_TEST_OBS"
            print_message "Actual total materialized view patients: $total_patient_flat, expected total: $TOTAL_VIEW_PATIENTS"
            print_message "Actual total materialized view encounters: $total_encounter_flat, expected total: $TOTAL_TEST_ENCOUNTERS"
            print_message "Actual total materialized view observations: $total_obs_flat, expected total: $TOTAL_TEST_OBS"
            exit 2
    fi
  else
    print_message "No parquet files available."
    exit 2
  fi
}

#######################################################################
# Function to clear json files if any created earlier by any other operation.
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#######################################################################
function clear() {
  rm -rf $HOME_PATH/$PARQUET_SUBDIR/*.json
}

#######################################################################
# Function to add resource on hapi server so that we can have some
# difference for incremental run.
# Globals:
#   SOURCE_FHIR_SERVER_URL
#######################################################################
function add_resource() {
  local fhir_username="hapi"
  local fhir_password="hapi"
  local fhir_url_extension="/fhir"

  curl -X POST -H "Content-Type: application/fhir+json; charset=utf-8" -u $fhir_username:$fhir_password \
  --connect-timeout 5 --max-time 20 "${SOURCE_FHIR_SERVER_URL}${fhir_url_extension}/Patient" \
  -d @resources/patient.json
}

#######################################################################
# Function to update resource on hapi server so that we can have some
# difference for incremental run.
# Globals:
#   SOURCE_FHIR_SERVER_URL
#   PATIENT_ID
#######################################################################
function update_resource() {
  local fhir_username="hapi"
  local fhir_password="hapi"
  local fhir_url_extension="/fhir"

  # Get patient id which we wish to modify.
  PATIENT_ID=$(curl -X GET -H "Content-Type: application/json; charset=utf-8" -u $fhir_username:$fhir_password \
  --connect-timeout 5 --max-time 20 "${SOURCE_FHIR_SERVER_URL}${fhir_url_extension}/Patient" \
   | jq '.entry' | jq -r '.[0].resource.id')

  print_message "Patient id which is being updated: ${PATIENT_ID}."

  # Update family name of the patient.
  curl -X PATCH -H "Content-Type: application/json-patch+json; charset=utf-8" -u $fhir_username:$fhir_password \
  --connect-timeout 5 --max-time 20 --data '[{ "op": "replace", "path": "/name/0/family", "value": "Anderson" }]' \
  "${SOURCE_FHIR_SERVER_URL}${fhir_url_extension}/Patient/${PATIENT_ID}"

  print_message "Patient ${PATIENT_ID} updated successfully."
}

#######################################################################
# Function to validate automatic creation of resource tables on pipeline run.
# Globals:
#   THRIFTSERVER_URL
#######################################################################
function validate_resource_tables() {
  beeline -u "jdbc:hive2://${THRIFTSERVER_URL}" -n hive -e 'show tables;' \
  --outputformat=csv2 >>hive_resource_tables.csv

  # Check for snapshot tables.
  if [[ $(grep patient_ hive_resource_tables.csv) && $(grep encounter_ hive_resource_tables.csv) \
      && $(grep observation_ hive_resource_tables.csv) ]]
  then
    print_message "Snapshot tables creation verified successfully."
  else
    print_message "Snapshot tables verification failed."
    exit 3
  fi

  # Check for canonical tables.
  if [[ $(grep -w patient hive_resource_tables.csv) && $(grep -w encounter hive_resource_tables.csv) \
      && $(grep -w observation hive_resource_tables.csv) ]]
  then
    print_message "Canonical tables creation verified successfully."
  else
    print_message "Canonical tables verification failed."
    exit 4
  fi
}

##############################################################
# Function to validate resource table data on pipeline run.
# Globals:
#   THRIFTSERVER_URL
##############################################################
function validate_resource_tables_data() {
  # Count all viral-load observations.
  local query="SELECT COUNT(0)
               FROM (
                 SELECT P.id AS pid, P.name.family AS family, P.gender AS gender, O.id AS obs_id,
                   OCC.code, O.status AS status, O.value.quantity.value
                 FROM Patient AS P, Observation AS O LATERAL VIEW explode(code.coding) AS OCC
                 WHERE P.id = O.subject.PatientId
                   AND OCC.code LIKE '856A%'
               );"
  beeline -u "jdbc:hive2://${THRIFTSERVER_URL}" -n hive -e "${query}" \
    --outputformat=csv2 >>hive_resource_tables_data.csv

  if [[ $(grep -w 265 hive_resource_tables_data.csv) ]]
  then
    print_message "Resource tables data verified successfully."
  else
    print_message "Resource tables data verification failed."
    exit 5
  fi
}

##############################################################
# Function to validate updated resource on pipeline run.
# When fetched from hapi server for patient id 13526, their given name comes as Shalon513
# and we have updated that to Sharon513, so we should verify Sharon513 in the patient table.
# Globals:
#   THRIFTSERVER_URL
#   PATIENT_ID
##############################################################
function validate_updated_resource() {
  # Count all viral-load observations.
  local query="SELECT p.name.family FROM patient AS p where p.id = ${PATIENT_ID};"
  beeline -u "jdbc:hive2://${THRIFTSERVER_URL}" -n hive -e "${query}" \
    --outputformat=csv2 >>patient_data.csv

  if [[ $(grep -w Anderson patient_data.csv) ]]
  then
    print_message "Updated patient data verified successfully."
  else
    print_message "Updated patient data verification failed."
    exit 6
  fi
}


function validate_updated_resource_in_fhir_sink() {
  local fhir_username="hapi"
  local fhir_password="hapi"
  local fhir_url_extension="/fhir"

  # Fetch the patient resource using the Patient ID
  local updated_family_name=$(curl -X GET -H "Content-Type: application/json; charset=utf-8" -u $fhir_username:$fhir_password \
  --connect-timeout 5 --max-time 20 "${SINK_FHIR_SERVER_URL}${fhir_url_extension}/Patient/${PATIENT_ID}" \
  | jq -r '.name[0].family')

  if [[ "${updated_family_name}" == "Anderson" ]]
  then
    print_message "Updated Patient data for ${PATIENT_ID} in FHIR sink verified successfully."
  else
    print_message "Updated Patient data verification for ${PATIENT_ID} in FHIR sink failed."
    exit 6
  fi
}


#################################################
# Function that counts resources in  FHIR server and compares output to what is
#  in the source FHIR server
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   SINK_FHIR_SERVER_URL
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
#################################################
function test_fhir_sink(){
  local runMode=$1

  query_fhir_server "${SINK_FHIR_SERVER_URL}"  "patients-sink.json" "encounters-sink.json" "obs-sink.json"

  print_message "Counting number of patients, encounters and obs sinked to fhir files"

  local total_patients_sinked_fhir=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/patients-sink.json")
  print_message "Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"

  local total_encounters_sinked_fhir=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/encounters-sink.json")
  print_message "Total encounters sinked to fhir ---> ${total_encounters_sinked_fhir}"

  local total_obs_sinked_fhir=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/obs-sink.json")
  print_message "Total observations sinked to fhir ---> ${total_obs_sinked_fhir}"

  if [[ "${total_patients_sinked_fhir}" == "${TOTAL_TEST_PATIENTS}" && "${total_encounters_sinked_fhir}" \
      == "${TOTAL_TEST_ENCOUNTERS}" && "${total_obs_sinked_fhir}" == "${TOTAL_TEST_OBS}" ]] \
  ; then
  print_message "FHIR SERVER SINK EXECUTED SUCCESSFULLY USING ${runMode} MODE"
  else
  print_message "FHIR SERVER SINK TEST FAILED USING ${runMode} MODE"
  exit 1
  fi
}

validate_args  "$@"
setup "$@"
fhir_source_query
sleep 30
# Full run.
run_pipeline "FULL"
wait_for_completion
if [[ "${DWH_TYPE}" == "PARQUET" ]]
then
  check_parquet false
else
  test_fhir_sink "FULL"
fi

clear

add_resource
update_resource
# Incremental run.
run_pipeline "INCREMENTAL"
wait_for_completion
if [[ "${DWH_TYPE}" == "PARQUET" ]]
then
  check_parquet true
  validate_resource_tables
  validate_resource_tables_data
  validate_updated_resource
  # View recreation run
  # TODO add validation for the views as well
  run_pipeline "VIEWS"
else
  fhir_source_query
  test_fhir_sink "INCREMENTAL"
  validate_updated_resource_in_fhir_sink
fi

print_message "END!!"