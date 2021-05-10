/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tools.ci.licensecheck;

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Checks the Jar files created by the build process. */
public class JarFileChecker {
    private static final Logger LOG = LoggerFactory.getLogger(JarFileChecker.class);

    public static int checkPath(Path path) throws Exception {
        List<Path> files = getBuildJars(path);
        LOG.info("Checking directory {} with a total of {} jar files.", path, files.size());

        int severeIssues = 0;
        for (Path file : files) {
            severeIssues += checkJar(file);
        }

        return severeIssues;
    }

    private static List<Path> getBuildJars(Path path) throws IOException {
        return Files.walk(path)
                .filter(file -> file.toString().endsWith(".jar"))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static int checkJar(Path file) throws Exception {
        final URI uri = file.toUri();

        int numSevereIssues = 0;
        try (final FileSystem fileSystem =
                FileSystems.newFileSystem(
                        new URI("jar:file", uri.getHost(), uri.getPath(), uri.getFragment()),
                        Collections.emptyMap())) {
            if (isTestJarAndEmpty(file, fileSystem.getPath("/"))) {
                return 0;
            }
            if (!noticeFileExistsAndIsValid(fileSystem.getPath("META-INF", "NOTICE"), file)) {
                numSevereIssues++;
            }
            if (!licenseFileExistsAndIsValid(fileSystem.getPath("META-INF", "LICENSE"), file)) {
                numSevereIssues++;
            }

            numSevereIssues +=
                    getNumLicenseFilesOutsideMetaInfDirectory(file, fileSystem.getPath("/"));

            numSevereIssues += getFilesWithIncompatibleLicenses(file, fileSystem.getPath("/"));
        }
        return numSevereIssues;
    }

    private static boolean isTestJarAndEmpty(Path jar, Path jarRoot) throws IOException {
        if (jar.getFileName().toString().endsWith("-tests.jar")) {
            try (Stream<Path> files = Files.walk(jarRoot)) {
                long numClassFiles =
                        files.filter(path -> !path.equals(jarRoot))
                                .filter(path -> path.getFileName().toString().endsWith(".class"))
                                .count();
                if (numClassFiles == 0) {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean noticeFileExistsAndIsValid(Path noticeFile, Path jar)
            throws IOException {
        if (!Files.exists(noticeFile)) {
            LOG.error("Missing META-INF/NOTICE in {}", jar);
            return false;
        }

        final String noticeFileContents =
                new String(Files.readAllBytes(noticeFile), StandardCharsets.UTF_8);
        if (!noticeFileContents.toLowerCase().contains("flink")
                || !noticeFileContents.contains("The Apache Software Foundation")) {
            LOG.error("The notice file in {} does not contain the expected entries.", jar);
            return false;
        }

        return true;
    }

    private static boolean licenseFileExistsAndIsValid(Path licenseFile, Path jar)
            throws IOException {
        if (!Files.exists(licenseFile)) {
            LOG.error("Missing META-INF/LICENSE in {}", jar);
            return false;
        }

        final String licenseFileContents =
                new String(Files.readAllBytes(licenseFile), StandardCharsets.UTF_8);
        if (!licenseFileContents.contains("Apache License")
                || !licenseFileContents.contains("Version 2.0, January 2004")) {
            LOG.error("The license file in {} does not contain the expected entries.", jar);
            return false;
        }

        return true;
    }

    private static int getFilesWithIncompatibleLicenses(Path jar, Path jarRoot) throws IOException {
        return findNonBinaryFilesContainingText(
                jar,
                jarRoot,
                asPatterns(
                        "GNU Lesser General Public License",
                        "GNU General Public License",
                        "GPL", // also detects LGPL
                        "GNU Affero General Public License",
                        "Amazon Software License",
                        "Confluent Community License Agreement Version 1.0",
                        "Don’t be evil") // can sometimes be found in "funny" licenses
                );
    }

    private static Collection<Pattern> asPatterns(String... texts) {
        return Stream.of(texts)
                .map(JarFileChecker::asPatternWithPotentialLineBreaks)
                .collect(Collectors.toList());
    }

    private static Pattern asPatternWithPotentialLineBreaks(String text) {
        return Pattern.compile(text.toLowerCase(Locale.ROOT).replaceAll(" ", " ?\\\\R?[\\\\s/#]*"));
    }

    private static int findNonBinaryFilesContainingText(
            Path jar, Path jarRoot, Collection<Pattern> forbidden) throws IOException {
        try (Stream<Path> files = Files.walk(jarRoot)) {
            return files.filter(path -> !path.equals(jarRoot))
                    .filter(path -> !Files.isDirectory(path))
                    .filter(JarFileChecker::isNoClassFile)
                    // frequent false-positives due to dual-licensing; generated by maven
                    .filter(path -> !getFileName(path).equals("dependencies"))
                    // false-positives due to dual-licensing; use startsWith to cover .txt/.md files
                    .filter(path -> !getFileName(path).startsWith("license"))
                    // false-positives due to optional components; startsWith covers .txt/.md files
                    .filter(path -> !getFileName(path).startsWith("notice"))
                    // dual-licensed under GPL 2 and CDDL 1.1
                    // contained in hadoop/presto S3 FS and flink-dist
                    .filter(path -> !pathStartsWith(path, "/META-INF/versions/11/javax/xml/bind"))
                    .filter(path -> !(isJavaxManifest(jar, path)))
                    // dual-licensed under GPL 2 and EPL 2.0
                    // contained in sql-avro-confluent-registry
                    .filter(path -> !pathStartsWith(path, "/org/glassfish/jersey/internal"))
                    .map(
                            path -> {
                                try {
                                    final String fileContents;
                                    try {
                                        fileContents =
                                                new String(
                                                                Files.readAllBytes(path),
                                                                StandardCharsets.UTF_8)
                                                        .toLowerCase();
                                    } catch (MalformedInputException mie) {
                                        // binary file
                                        return 0;
                                    }

                                    int violations = 0;
                                    for (Pattern text : forbidden) {
                                        if (text.matcher(fileContents).find()) {
                                            // do not count individual violations because it can be
                                            // confusing when checking with aliases for the same
                                            // license
                                            violations = 1;
                                            LOG.error(
                                                    "File '{}' in jar '{}' contains forbidden text '{}'.",
                                                    path,
                                                    jar,
                                                    text);
                                        }
                                    }
                                    return violations;
                                } catch (IOException e) {
                                    throw new RuntimeException(
                                            String.format(
                                                    "Could not read contents of file '%s' in jar '%s'.",
                                                    path, jar),
                                            e);
                                }
                            })
                    .reduce(Integer::sum)
                    .orElse(0);
        }
    }

    private static int getNumLicenseFilesOutsideMetaInfDirectory(Path jar, Path jarRoot)
            throws IOException {
        try (Stream<Path> files = Files.walk(jarRoot)) {
            /*
             * LICENSE or NOTICE files found outside of the META-INF directory are most likely shading mistakes (we are including the files from other dependencies, thus providing an invalid LICENSE file)
             *
             * <p>In such a case, we recommend updating the shading exclusions, and adding the license file to META-INF/licenses.
             */
            final List<String> filesWithIssues =
                    files.filter(path -> !path.equals(jarRoot))
                            .filter(
                                    path ->
                                            getFileName(path).contains("license")
                                                    || getFileName(path).contains("notice"))
                            .filter(
                                    path ->
                                            !Files.isDirectory(
                                                    path)) // ignore directories, e.g. "license/"
                            .filter(JarFileChecker::isNoClassFile) // some class files contain
                            // LICENSE in their name
                            .filter(
                                    path ->
                                            !getFileName(path)
                                                    .endsWith(".ftl")) // a false positive in
                            // flink-python
                            .map(Path::toString)
                            .filter(
                                    path ->
                                            !path.contains(
                                                    "META-INF")) // license files in META-INF are
                            // expected
                            .filter(
                                    path ->
                                            !path.endsWith(
                                                    "web/3rdpartylicenses.txt")) // a false positive
                            // in
                            // flink-runtime-web
                            .collect(Collectors.toList());
            for (String fileWithIssue : filesWithIssues) {
                LOG.error(
                        "Jar file {} contains a LICENSE file in an unexpected location: {}",
                        jar,
                        fileWithIssue);
            }
            return filesWithIssues.size();
        }
    }

    private static String getFileName(Path path) {
        return path.getFileName().toString().toLowerCase();
    }

    private static boolean pathStartsWith(Path file, String path) {
        return file.startsWith(file.getFileSystem().getPath(path));
    }

    private static boolean equals(Path file, String path) {
        return file.equals(file.getFileSystem().getPath(path));
    }

    private static boolean isNoClassFile(Path file) {
        return !getFileName(file).endsWith(".class");
    }

    private static boolean isJavaxManifest(Path jar, Path potentialManifestFile) {
        final String jarFileName = getFileName(jar);

        return (jarFileName.startsWith("flink-s3-fs-hadoop")
                        || jarFileName.startsWith("flink-s3-fs-presto"))
                && equals(potentialManifestFile, "/META-INF/versions/11/META-INF/MANIFEST.MF");
    }
}
