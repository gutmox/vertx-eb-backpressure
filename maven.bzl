load("@rules_jvm_external//:defs.bzl", "maven_install")

def maven():
    maven_install(
        name = "maven",
        artifacts = [
            "io.vertx:vertx-web:3.9.1",
            "io.vertx:vertx-core:3.9.1",
            "io.vertx:vertx-rx-java2:3.9.1",
        ],
        repositories = [
            "https://maven.google.com",
            "https://repo1.maven.org/maven2",
        ],
        fetch_sources = True,
    )

    maven_install(
	    name = "maven_test",
	    artifacts = [
		    "io.vertx:vertx-junit5:3.9.1",
		    "org.assertj:assertj-core:3.16.1",
		    "org.junit.jupiter:junit-jupiter-engine:5.6.2",
		    "org.junit.jupiter:junit-jupiter-api:5.6.2",
		    "org.junit.platform:junit-platform-console:1.5.2",
	    ],
	    repositories = [
		    "https://maven.google.com",
		    "https://repo1.maven.org/maven2",
	    ],
	    fetch_sources = True,
    )