#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

END_TO_END_DIR="`dirname \"$0\"`" # relative
END_TO_END_DIR="`( cd \"$END_TO_END_DIR\" && pwd -P)`" # absolutized and normalized
if [ -z "$END_TO_END_DIR" ] ; then
    # error; for some reason, the path is not accessible
    # to the script (e.g. permissions re-evaled after suid)
    exit 1  # fail
fi

export END_TO_END_DIR

if [ -z "$FLINK_DIR" ] ; then
    echo "You have to export the Flink distribution directory as FLINK_DIR"
    exit 1
fi

source "${END_TO_END_DIR}/../tools/ci/maven-utils.sh"
source "${END_TO_END_DIR}/test-scripts/test-runner-common.sh"

# On Azure CI, set artifacts dir
if [ ! -z "$TF_BUILD" ] ; then
	export ARTIFACTS_DIR="${END_TO_END_DIR}/artifacts"
	mkdir -p $ARTIFACTS_DIR || { echo "FAILURE: cannot create log directory '${ARTIFACTS_DIR}'." ; exit 1; }

	function run_on_exit {
		collect_coredumps $(pwd) $ARTIFACTS_DIR
		compress_logs
	}

	# compress and register logs for publication on exit
	function compress_logs {
		echo "COMPRESSING build artifacts."
		COMPRESSED_ARCHIVE=${BUILD_BUILDNUMBER}.tgz
		mkdir compressed-archive-dir
		tar -zcvf compressed-archive-dir/${COMPRESSED_ARCHIVE} -C $ARTIFACTS_DIR .
		echo "##vso[task.setvariable variable=ARTIFACT_DIR]$(pwd)/compressed-archive-dir"
	}
	on_exit run_on_exit
fi

FLINK_DIR="`( cd \"$FLINK_DIR\" && pwd -P)`" # absolutized and normalized

echo "flink-end-to-end-test directory: $END_TO_END_DIR"
echo "Flink distribution directory: $FLINK_DIR"

echo "Java and Maven version"
java -version
run_mvn -version

echo "Free disk space"
df -h

echo "Running with profile '$PROFILE'"

# Template for adding a test:

# run_test "<description>" "$END_TO_END_DIR/test-scripts/<script_name>" ["skip_check_exceptions"]

# IMPORTANT:
# With the "skip_check_exceptions" flag one can disable default exceptions and errors checking in log files. This should be done
# carefully though. A valid reasons for doing so could be e.g killing TMs randomly as we cannot predict what exception could be thrown. Whenever
# those checks are disabled, one should take care that a proper checks are performed in the tests itself that ensure that the test finished
# in an expected state.

printf "\n\n==============================================================================\n"
printf "Running bash end-to-end tests\n"
printf "==============================================================================\n"

################################################################################
# Checkpointing tests
################################################################################



################################################################################
# Docker / Container / Kubernetes / Mesos tests
################################################################################

# These tests are known to fail on JDK11. See FLINK-13719
if [[ ${PROFILE} != *"jdk11"* ]]; then


	if [[ `uname -i` != 'aarch64' ]]; then

		
		# Hadoop YARN deosn't support aarch64 at this moment. See: https://issues.apache.org/jira/browse/HADOOP-16723
		run_test "Running Kerberized YARN per-job on Docker test (default input)" "$END_TO_END_DIR/test-scripts/test_yarn_job_kerberos_docker.sh"
		run_test "Running Kerberized YARN per-job on Docker test (custom fs plugin)" "$END_TO_END_DIR/test-scripts/test_yarn_job_kerberos_docker.sh dummy-fs"
		run_test "Running Kerberized YARN application on Docker test (default input)" "$END_TO_END_DIR/test-scripts/test_yarn_application_kerberos_docker.sh"
		run_test "Running Kerberized YARN application on Docker test (custom fs plugin)" "$END_TO_END_DIR/test-scripts/test_yarn_application_kerberos_docker.sh dummy-fs"
	fi
fi


if [[ `uname -i` != 'aarch64' ]]; then
    run_test "PyFlink end-to-end test" "$END_TO_END_DIR/test-scripts/test_pyflink.sh" "skip_check_exceptions"
fi

