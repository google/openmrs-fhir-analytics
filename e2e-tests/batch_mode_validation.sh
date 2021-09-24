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
#   ./batch_mode_validation.sh ./ JDBC
#   ./batch_mode_validation.sh ./ NON-JDBC --use_docker_network

set -e

#################################################
# Prints the usage
#################################################
function usage() {
  echo "This script validates if number of resources sunk in parquet files and" 
  echo "FHIR Server match what is stored in the OpenMRS server"
  echo 
  echo " usage: ./batch_mode_validation.sh  HOME_DIR  PARQUET_SUBDIR  [OPTIONS] "
  echo "    HOME_DIR          Path where e2e-tests directory is. Directory MUST"
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
  if [[ $# -lt 2 || $# -gt 3  ]]; then
    echo "Invalid number of args passed."
    usage
    exit 1
  fi

  echo "Checking if the Parquet-tools JAR exists..."
  if compgen -G "${1}/parquet-tools*.jar" > /dev/null; then
    echo "Parquet-tools JAR exists in ${1}"
  else
    echo "Parquet-tools JAR not found in ${1}"
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
  local print_prefix="E2E: BATCH MODE TEST:"
  echo "${print_prefix} $*"
}

#################################################
# Function that defines the global vars
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   OPENMRS_URL
#   SINK_SERVER
# Arguments:
#   Path where e2e-tests directory is. Directory contains parquet tools jar as 
#      well as subdirectory of parquet file output
#   Subdirectory name under HOME_DIR containing parquet files. 
#      Example: NON_JDBC or JDBC
#   Optional: Flag to specify whether to use docker or host network URLs.
#################################################
function setup() {
  HOME_PATH=$1
  PARQUET_SUBDIR=$2
  rm -rf ${HOME_PATH}/fhir
  rm -rf ${HOME_PATH}/${PARQUET_SUBDIR}/*.json
  
  OPENMRS_URL='http://localhost:8099'
  SINK_SERVER='http://localhost:8098'

  if [[ $3 = "--use_docker_network" ]]; then
    OPENMRS_URL='http://openmrs:8080'
    SINK_SERVER='http://sink-server:8080'
  fi
}

#################################################
# Function to count resources in openmrs server
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   OPENMRS_URL
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
#################################################
function openmrs_query() {
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    ${OPENMRS_URL}/openmrs/ws/fhir2/R4/Patient/ 2>/dev/null >>${HOME_PATH}/${PARQUET_SUBDIR}/patients.json
  TOTAL_TEST_PATIENTS=$(jq '.total' ${HOME_PATH}/${PARQUET_SUBDIR}/patients.json)
  print_message "Total openmrs test patients ---> ${TOTAL_TEST_PATIENTS}"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    ${OPENMRS_URL}/openmrs/ws/fhir2/R4/Encounter/ 2>/dev/null >>${HOME_PATH}/${PARQUET_SUBDIR}/encounters.json
  TOTAL_TEST_ENCOUNTERS=$(jq '.total' ${HOME_PATH}/${PARQUET_SUBDIR}/encounters.json)
  print_message "Total openmrs test encounters ---> ${TOTAL_TEST_ENCOUNTERS}"
  curl -L -X GET -u admin:Admin123 --connect-timeout 5 --max-time 20 \
    ${OPENMRS_URL}/openmrs/ws/fhir2/R4/Observation/ 2>/dev/null >>${HOME_PATH}/${PARQUET_SUBDIR}/obs.json
  TOTAL_TEST_OBS=$(jq '.total' ${HOME_PATH}/${PARQUET_SUBDIR}/obs.json)
  print_message "Total openmrs test obs ---> ${TOTAL_TEST_OBS}"
}


#################################################
# Function that counts resources in parquet files and compares output to what 
#  is in openmrs server
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
#################################################
function test_parquet_sink() {
  print_message "Counting number of patients, encounters and obs sinked to parquet files"
  
  total_patients_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ${HOME_PATH}/${PARQUET_SUBDIR}/Patient/ | awk '{print $3}')
  print_message "Total patients synced to parquet ---> ${total_patients_streamed}"
  total_encounters_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ${HOME_PATH}/${PARQUET_SUBDIR}/Encounter/ | awk '{print $3}')
  print_message "Total encounters synced to parquet ---> ${total_encounters_streamed}"
  total_obs_streamed=$(java -jar ./parquet-tools-1.11.1.jar rowcount ${HOME_PATH}/${PARQUET_SUBDIR}/Observation/ | awk '{print $3}')
  print_message "Total obs synced to parquet ---> ${total_obs_streamed}"

  if [[ ${total_patients_streamed} == ${TOTAL_TEST_PATIENTS} && ${total_encounters_streamed} \
        == ${TOTAL_TEST_ENCOUNTERS} && ${total_obs_streamed} == ${TOTAL_TEST_OBS} ]] \
    ; then
    print_message "BATCH MODE WITH PARQUET SINK EXECUTED SUCCESSFULLY USING ${PARQUET_SUBDIR} MODE"
  else
    print_message "BATCH MODE WITH PARQUET SINK TEST FAILED USING ${PARQUET_SUBDIR} MODE"
    exit 1
  fi
}

#################################################
# Function that counts resources in  FHIR server and compares output to what is 
#  in openmrs server
# Globals:
#   HOME_PATH
#   PARQUET_SUBDIR
#   SINK_SERVER
#   TOTAL_TEST_PATIENTS
#   TOTAL_TEST_ENCOUNTERS
#   TOTAL_TEST_OBS
#################################################
function test_fhir_sink() {
  print_message "Finding number of patients, encounters and obs in FHIR server"

  mkdir ${HOME_PATH}/fhir
  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    ${SINK_SERVER}/fhir/Patient/?_summary=count 2>/dev/null >>${HOME_PATH}/fhir/patients.json

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    ${SINK_SERVER}/fhir/Encounter/?_summary=count 2>/dev/null >>${HOME_PATH}/fhir/encounters.json

  curl -L -X GET -u hapi:hapi --connect-timeout 5 --max-time 20 \
    ${SINK_SERVER}/fhir/Observation/?_summary=count 2>/dev/null >>${HOME_PATH}/fhir/obs.json

  print_message "Counting number of patients, encounters and obs sinked to fhir files"

  total_patients_sinked_fhir=$(jq '.total' ${HOME_PATH}/fhir/patients.json)
  print_message "Total patients sinked to fhir ---> ${total_patients_sinked_fhir}"

  total_encounters_sinked_fhir=$(jq '.total' ${HOME_PATH}/fhir/encounters.json)
  print_message "Total encounters sinked to fhir ---> ${total_encounters_sinked_fhir}"

  total_obs_sinked_fhir=$(jq '.total' ${HOME_PATH}/fhir/obs.json)
  print_message "Total observations sinked to fhir ---> ${total_obs_sinked_fhir}"

  if [[ ${total_patients_sinked_fhir} == ${TOTAL_TEST_PATIENTS} && ${total_encounters_sinked_fhir} \
        == ${TOTAL_TEST_ENCOUNTERS} && ${total_obs_sinked_fhir} == ${TOTAL_TEST_OBS} ]] \
    ; then
    print_message "BATCH MODE WITH FHIR SERVER SINK EXECUTED SUCCESSFULLY USING ${PARQUET_SUBDIR} MODE"
  else
    print_message "BATCH MODE WITH FHIR SERVER SINK TEST FAILED USING ${PARQUET_SUBDIR} MODE"
    exit 1
  fi
}

validate_args  $@
setup $@
print_message "---- STARTING ${PARQUET_SUBDIR} TEST ----"
openmrs_query 
test_parquet_sink 
test_fhir_sink 
print_message "END!!"