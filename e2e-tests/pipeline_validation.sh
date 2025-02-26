#!/usr/bin/env bash

# Copyright 2021 Google LLC
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
#   ./pipeline_validation.sh ./ JDBC_OPENMRS --openmrs
#   ./pipeline_validation.sh ./ FHIR_SEARCH_HAPI --use_docker_network
#   ./pipeline_validation.sh ./ STREAMING --use_docker_network

set -e

#################################################
# Prints the usage
#################################################
function usage() {
  echo "This script validates if number of resources sunk in parquet files and"
  echo "sink FHIR server match what is stored in the source FHIR server"
  echo
  echo " usage: ./pipeline_validation.sh  HOME_DIR  PARQUET_SUBDIR  [OPTIONS] "
  echo "    HOME_DIR          Path where e2e-tests directory is. Directory MUST"
  echo "                      contain the parquet tools jar as well as subdirectory"
  echo "                      of parquet file output"
  echo "    PARQUET_SUBDIR    Subdirectory name under HOME_DIR containing"
  echo "                      parquet files  "
  echo
  echo " Options:  "
  echo "     --use_docker_network     Flag to specify whether to use docker"
  echo "                              or host network URLs"
  echo "     --streaming              Flag to specify whether we are testing a"
  echo "                              streaming pipeline"
  echo "     --openmrs                Flag to specify whether we are testing a"
  echo "                              batch pipeline with OpenMRS as the source"
  echo "                              server"
}

#################################################
# Makes sure args passed are correct
#################################################
function validate_args() {
  if [[ $# -lt 2 || $# -gt 7  ]]; then
    echo "Invalid number of args passed."
    usage
    exit 1
  fi

  echo "Checking if the Parquet-tools JAR exists..."
  if [[ -n $( find "${1}/controller-spark" -name parquet-tools*.jar) ]]
  then
    echo "Parquet-tools JAR exists in ${1}/controller-spark"
  else
    echo "Parquet-tools JAR not found in ${1}/controller-spark"
    usage
    exit 1
  fi

  if [[ ! -d ${1}/${2} ]]; then
    echo "The directory ${1}/${2} does not exist."
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
  local print_prefix="E2E TEST:"
  echo "${print_prefix} $*"
}

#################################################
# Function that defines the global vars
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   SOURCE_FHIR_SERVER_URL
#   SINK_FHIR_SERVER_URL
#   STREAMING
#   OPENMRS
# Arguments:
#   Path where e2e-tests directory is. Directory contains parquet tools jar as
#      well as subdirectory of parquet file output
#   Subdirectory name under HOME_DIR containing parquet files.
#      Example: FHIR_SEARCH or JDBC_OPENMRS
#   Optional: Flag to specify whether to use docker or host network URLs.
#   Optional: Flag to specify streaming pipeline test.
#   Optional: Flag to specify whether the source server is OpenMRS.
#################################################
function setup() {
  HOME_PATH=$1
  PARQUET_SUBDIR=$2
  FHIR_JSON_SUBDIR=$3
  SINK_FHIR_SERVER_URL=$4
  rm -rf "${HOME_PATH}/${FHIR_JSON_SUBDIR}"
  rm -rf "${HOME_PATH}/${PARQUET_SUBDIR}/*.json"
  find "${HOME_PATH}/${PARQUET_SUBDIR}" -size 0 -delete
  SOURCE_FHIR_SERVER_URL='http://localhost:8091'
  STREAMING=""
  OPENMRS=""

  # TODO: We should refactor this code to parse the arguments by going through
  # each one and checking which ones are turned on.
  if [[ $5 = "--openmrs" ]] || [[ $6 = "--openmrs" ]] || [[ $7 = "--openmrs" ]]; then
    OPENMRS="on"
    SOURCE_FHIR_SERVER_URL='http://localhost:8099/openmrs/ws/fhir2/R4'
  fi

  if [[ $5 = "--use_docker_network" ]] || [[ $6 = "--use_docker_network" ]] || [[ $7 = "--use_docker_network" ]]; then
    if [[ -n ${OPENMRS} ]]; then
        SOURCE_FHIR_SERVER_URL='http://openmrs:8080/openmrs/ws/fhir2/R4'
    else
        SOURCE_FHIR_SERVER_URL='http://hapi-server:8080'
    fi
  fi

  # TODO: the streaming mode is currently not tested as it was removed; we have
  # kept this logic around since we may add streaming mode in the Beam pipeline.
  if [[ $5 = "--streaming" ]] || [[ $6 = "--streaming" ]] || [[ $7 = "--streaming" ]]; then
    STREAMING="on"
  fi
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
#   STREAMING
#   OPENMRS
#################################################
function fhir_source_query() {
  local patient_query_param="?_summary=count"
  local enc_obs_query_param="?_summary=count"
  local fhir_username="hapi"
  local fhir_password="hapi"
  local fhir_url_extension="/fhir"

  if [[ -n ${STREAMING} ]]; then
      patient_query_param="?given=Alberta625"
      enc_obs_query_param="?subject.given=Alberta625"
  fi

  if [[ -n ${OPENMRS} ]]; then
      fhir_username="admin"
      fhir_password="Admin123"
      fhir_url_extension=""
  fi

  curl -L -X GET -u $fhir_username:$fhir_password --connect-timeout 5 --max-time 20 \
  "${SOURCE_FHIR_SERVER_URL}${fhir_url_extension}/Patient${patient_query_param}" 2>/dev/null \
  >>"${HOME_PATH}/${PARQUET_SUBDIR}/patients.json"
  TOTAL_TEST_PATIENTS=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/patients.json")
  print_message "Total FHIR source test patients ---> ${TOTAL_TEST_PATIENTS}"

  curl -L -X GET -u $fhir_username:$fhir_password --connect-timeout 5 --max-time 20 \
    "${SOURCE_FHIR_SERVER_URL}${fhir_url_extension}/Encounter${enc_obs_query_param}" \
    2>/dev/null >>"${HOME_PATH}/${PARQUET_SUBDIR}/encounters.json"
  TOTAL_TEST_ENCOUNTERS=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/encounters.json")
  print_message "Total FHIR source test encounters ---> ${TOTAL_TEST_ENCOUNTERS}"

  curl -L -X GET -u $fhir_username:$fhir_password --connect-timeout 5 --max-time 20 \
    "${SOURCE_FHIR_SERVER_URL}${fhir_url_extension}/Observation${enc_obs_query_param}" \
    2>/dev/null >>"${HOME_PATH}/${PARQUET_SUBDIR}/obs.json"
  TOTAL_TEST_OBS=$(jq '.total' "${HOME_PATH}/${PARQUET_SUBDIR}/obs.json")
  print_message "Total FHIR source test obs ---> ${TOTAL_TEST_OBS}"
}


#################################################
# Function that counts resources in parquet files and compares output to what
#  is in source FHIR server
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
#   OPENMRS
#################################################
function test_parquet_sink() {
  # This global variable is hardcoded to validate the View record count
  # which can greater than the number of Resources in the source FHIR
  # Server due to flattening
  PATIENT_VIEW_ROWCOUNT=106
  OBS_VIEW_ROWCOUNT=${TOTAL_TEST_OBS}
  if [[ -n ${OPENMRS} ]]; then
    PATIENT_VIEW_ROWCOUNT=108
    OBS_VIEW_ROWCOUNT=284379
  fi


  print_message "Counting number of patients, encounters and obs sinked to parquet files"
  local total_patients_streamed=$(java -Xms16g -Xmx16g -jar \
  ./controller-spark/parquet-tools-1.11.1.jar rowcount "${HOME_PATH}/${PARQUET_SUBDIR}/Patient/" | \
  awk '{print $3}')
  print_message "Total patients synced to parquet ---> ${total_patients_streamed}"

  local total_encounters_streamed=$(java -Xms16g -Xmx16g -jar \
  ./controller-spark/parquet-tools-1.11.1.jar rowcount "${HOME_PATH}/${PARQUET_SUBDIR}/Encounter/" \
  | awk '{print $3}')
  print_message "Total encounters synced to parquet ---> ${total_encounters_streamed}"

  local total_obs_streamed=$(java -Xms16g -Xmx16g -jar ./controller-spark/parquet-tools-1.11.1.jar \
  rowcount "${HOME_PATH}/${PARQUET_SUBDIR}/Observation/" | awk '{print $3}')
  print_message "Total obs synced to parquet ---> ${total_obs_streamed}"

  if [[ ! (-n ${STREAMING}) ]]; then
    print_message "Parquet Sink Test Non-Streaming mode"
    local total_patient_flat=$(java -Xms16g -Xmx16g -jar \
    ./controller-spark/parquet-tools-1.11.1.jar rowcount "${HOME_PATH}/${PARQUET_SUBDIR}/patient_flat/" | \
    awk '{print $3}')
    print_message "Total patient flat rows synced to parquet ---> ${total_patient_flat}"

    local total_encounter_flat=$(java -Xms16g -Xmx16g -jar \
    ./controller-spark/parquet-tools-1.11.1.jar rowcount "${HOME_PATH}/${PARQUET_SUBDIR}/encounter_flat/" \
    | awk '{print $3}')
    print_message "Total encounter flat rows synced to parquet ---> ${total_encounter_flat}"

    local total_obs_flat=$(java -Xms16g -Xmx16g -jar ./controller-spark/parquet-tools-1.11.1.jar \
    rowcount "${HOME_PATH}/${PARQUET_SUBDIR}/observation_flat/" | awk '{print $3}')
    print_message "Total observation flat rows synced to parquet ---> ${total_obs_flat}"

    if (( total_patients_streamed == TOTAL_TEST_PATIENTS && total_encounters_streamed \
        == TOTAL_TEST_ENCOUNTERS && total_obs_streamed == TOTAL_TEST_OBS \
        && total_obs_flat == OBS_VIEW_ROWCOUNT && \
        total_patient_flat == PATIENT_VIEW_ROWCOUNT && \
        total_encounter_flat == TOTAL_TEST_ENCOUNTERS )) ; then
        print_message "PARQUET SINK EXECUTED SUCCESSFULLY USING ${PARQUET_SUBDIR} MODE"
      else
        print_message "PARQUET SINK TEST FAILED USING ${PARQUET_SUBDIR} MODE"
        exit 1
    fi
  elif (( total_patients_streamed == TOTAL_TEST_PATIENTS && total_encounters_streamed \
        == TOTAL_TEST_ENCOUNTERS && total_obs_streamed == TOTAL_TEST_OBS )) ; then
        print_message "PARQUET SINK EXECUTED SUCCESSFULLY USING ${PARQUET_SUBDIR} MODE"
      else
        print_message "PARQUET SINK TEST FAILED USING ${PARQUET_SUBDIR} MODE"
        exit 1
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
#   STREAMING
#   OPENMRS
#################################################
function test_fhir_sink() {
  # This skips the test
  if [ "$SINK_FHIR_SERVER_URL" = "NONE" ]; then
    return
  fi

  local patient_query_param="?_summary=count"
  local enc_obs_query_param="?_summary=count"

  if [[ -n ${STREAMING} ]]; then
      patient_query_param="?given=Alberta625&_summary=count"
      enc_obs_query_param="?subject.given=Alberta625&_summary=count"
  fi
  print_message "Finding number of patients, encounters and obs in FHIR server"

  if [ -d "${HOME_PATH}/${FHIR_JSON_SUBDIR}/fhir" ]; then
      print_message "Directory containing fhir resources already exists ---> ${HOME_PATH}/${FHIR_JSON_SUBDIR}/fhir"
      exit 1
  fi

  mkdir -p "${HOME_PATH}/${FHIR_JSON_SUBDIR}/fhir"
  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    "${SINK_FHIR_SERVER_URL}/fhir/Patient${patient_query_param}" 2>/dev/null >>"${HOME_PATH}/${FHIR_JSON_SUBDIR}/fhir/patients.json"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    "${SINK_FHIR_SERVER_URL}/fhir/Encounter${enc_obs_query_param}" 2>/dev/null >>"${HOME_PATH}/${FHIR_JSON_SUBDIR}/fhir/encounters.json"

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    "${SINK_FHIR_SERVER_URL}/fhir/Observation${enc_obs_query_param}" 2>/dev/null >>"${HOME_PATH}/${FHIR_JSON_SUBDIR}/fhir/obs.json"

  print_message "Counting number of patients, encounters and obs sinked to fhir files"

  local total_patients_sinked_fhir=$(jq '.total' "${HOME_PATH}/${FHIR_JSON_SUBDIR}/fhir/patients.json")
  print_message "Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"

  local total_encounters_sinked_fhir=$(jq '.total' "${HOME_PATH}/${FHIR_JSON_SUBDIR}/fhir/encounters.json")
  print_message "Total encounters sinked to fhir ---> ${total_encounters_sinked_fhir}"

  local total_obs_sinked_fhir=$(jq '.total' "${HOME_PATH}/${FHIR_JSON_SUBDIR}/fhir/obs.json")
  print_message "Total observations sinked to fhir ---> ${total_obs_sinked_fhir}"

  if [[ "${total_patients_sinked_fhir}" == "${TOTAL_TEST_PATIENTS}" && "${total_encounters_sinked_fhir}" \
        == "${TOTAL_TEST_ENCOUNTERS}" && "${total_obs_sinked_fhir}" == "${TOTAL_TEST_OBS}" ]] \
    ; then
    print_message "FHIR SERVER SINK EXECUTED SUCCESSFULLY USING ${PARQUET_SUBDIR} MODE"
  else
    print_message "FHIR SERVER SINK TEST FAILED USING ${PARQUET_SUBDIR} MODE"
    exit 1
  fi
}

validate_args  "$@"
setup "$@"
print_message "---- STARTING ${PARQUET_SUBDIR} TEST ----"
fhir_source_query
test_parquet_sink
test_fhir_sink
print_message "END!!"
