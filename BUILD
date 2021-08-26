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

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_bazel_distribution//artifact:rules.bzl", "artifact_extractor")

exports_files(["VERSION", "LICENSE", "README.md"])

artifact_extractor(
    name = "typedb-extractor-linux",
    artifact = "@vaticle_typedb_artifact_linux//file",
)

java_library(
    name = "migrator-src",
    srcs = glob(["*.java", "writer/*.java"]),
    deps = [
        # External dependencies from Grakn Labs
        "@vaticle_typedb_common//:common",
        "@vaticle_typedb_client_java//api:api",
        "@vaticle_typedb_client_java//:client-java",
        "@vaticle_typeql_lang_java//:typeql-lang",
        "@vaticle_typeql_lang_java//pattern:pattern",
        "@vaticle_typeql_lang_java//query:query",

        # External dependencies from Maven
        "@maven//:org_apache_commons_commons_csv",
        "@maven//:info_picocli_picocli",
        "@maven//:org_slf4j_slf4j_api",
    ],
)

java_binary(
    name = "migrator",
    main_class = "biograkn.semmed.Migrator",
    runtime_deps = [":migrator-src"],
    tags = ["maven_coordinates=io.grakn.biograkn:biograkn-semmed:{pom_version}"],
    resources = [ "//conf:logback.xml" ],
    data = ["//schema:biograkn-semmed.gql"],
    resource_strip_prefix = "conf/",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", ".grabl/*", "writer/*"]),
    license_type = "apache",
)