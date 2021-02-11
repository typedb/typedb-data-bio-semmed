#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

workspace(name = "graknlabs_biograkn_semmed")

################################
# Load @graknlabs_dependencies #
################################

load("//dependencies/graknlabs:repositories.bzl", "graknlabs_dependencies")
graknlabs_dependencies()

# Load //builder/bazel for RBE
load("@graknlabs_dependencies//builder/bazel:deps.bzl", "bazel_toolchain")
bazel_toolchain()

# Load //builder/java
load("@graknlabs_dependencies//builder/java:deps.bzl", java_deps = "deps")
java_deps()

# Load //builder/kotlin
load("@graknlabs_dependencies//builder/kotlin:deps.bzl", kotlin_deps = "deps")
kotlin_deps()
load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kotlin_repositories", "kt_register_toolchains")
kotlin_repositories()
kt_register_toolchains()

# Load //builder/python
load("@graknlabs_dependencies//builder/python:deps.bzl", python_deps = "deps")
python_deps()

# Load //builder/antlr
load("@graknlabs_dependencies//builder/antlr:deps.bzl", antlr_deps = "deps")
antlr_deps()
load("@rules_antlr//antlr:deps.bzl", "antlr_dependencies")
antlr_dependencies()

# Load //builder/grpc
load("@graknlabs_dependencies//builder/grpc:deps.bzl", grpc_deps = "deps")
grpc_deps()
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl",
com_github_grpc_grpc_deps = "grpc_deps")
com_github_grpc_grpc_deps()
load("@stackb_rules_proto//java:deps.bzl", "java_grpc_compile")
java_grpc_compile()

# Load //tool/common
load("@graknlabs_dependencies//tool/common:deps.bzl", "graknlabs_dependencies_ci_pip",
    graknlabs_dependencies_tool_maven_artifacts = "maven_artifacts")
graknlabs_dependencies_ci_pip()

# Load //tool/checkstyle
load("@graknlabs_dependencies//tool/checkstyle:deps.bzl", checkstyle_deps = "deps")
checkstyle_deps()

# Load //tool/unuseddeps
load("@graknlabs_dependencies//tool/unuseddeps:deps.bzl", unuseddeps_deps = "deps")
unuseddeps_deps()

######################################
# Load @graknlabs_bazel_distribution #
######################################

load("@graknlabs_dependencies//distribution:deps.bzl", "graknlabs_bazel_distribution")
graknlabs_bazel_distribution()

# Load //common
load("@graknlabs_bazel_distribution//common:deps.bzl", "rules_pkg")
rules_pkg()
load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")
rules_pkg_dependencies()

################################
# Load @graknlabs dependencies #
################################

load("//dependencies/graknlabs:repositories.bzl", "graknlabs_common", "graknlabs_client_java")
graknlabs_common()
graknlabs_client_java()

# @graknlbs_client_java dependencies
load("@graknlabs_client_java//dependencies/graknlabs:repositories.bzl", "graknlabs_graql", "graknlabs_protocol", "graknlabs_grabl_tracing")
graknlabs_graql()
graknlabs_protocol()
graknlabs_grabl_tracing()

load("//dependencies/graknlabs:artifacts.bzl", "graknlabs_grakn_core_artifacts")
graknlabs_grakn_core_artifacts()

############################
# Load @maven dependencies #
############################

load("//dependencies/maven:artifacts.bzl", graknlabs_biograkn_semmed_artifacts = "artifacts")
load("@graknlabs_dependencies//tool/common:deps.bzl", graknlabs_dependencies_tool_common_artifacts = "maven_artifacts")
load("@graknlabs_common//dependencies/maven:artifacts.bzl", graknlabs_common_artifacts = "artifacts")
load("@graknlabs_client_java//dependencies/maven:artifacts.bzl", graknlabs_client_java_artifacts = "artifacts")
load("@graknlabs_graql//dependencies/maven:artifacts.bzl", graknlabs_graql_artifacts = "artifacts")
load("@graknlabs_protocol//dependencies/maven:artifacts.bzl", graknlabs_protocol_artifacts = "artifacts")
load("@graknlabs_grabl_tracing//dependencies/maven:artifacts.bzl", graknlabs_grabl_tracing_artifacts = "artifacts")

load("@graknlabs_dependencies//library/maven:rules.bzl", "maven")
maven(
    graknlabs_biograkn_semmed_artifacts +
    graknlabs_dependencies_tool_common_artifacts +
    graknlabs_common_artifacts +
    graknlabs_client_java_artifacts +
    graknlabs_graql_artifacts +
    graknlabs_protocol_artifacts +
    graknlabs_grabl_tracing_artifacts
)
