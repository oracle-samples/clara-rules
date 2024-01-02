# How to Release

This project is hosted on [Clojars][clojars].  You can see it [here][release-site].

Releasing the project requires these steps:

1. Run ```make test``` to ensure everything is working as expected.
2. Set the version number and tag in the `pom.xml` file, commit the changes.
3. Use a GitHub [project release][github-release-url] to release the project and tag (be sure it follows [semver][semantic-versioning])
4. Run ```make clean build``` to test building the project, commit any changes to the `pom.xml` file.
5. Run ```make deploy``` to deploy the project to the Clojars repository.
6. Update the `pom.xml` in `main` to a new minor version, commit the changes.

[clojars]: https://clojars.org
[release-site]: https://clojars.org/com.github.k13labs/clara-rules
[project-url]: https://github.com/k13labs/clara-rules/
[semantic-versioning]: http://semver.org/
[github-release-url]: https://help.github.com/articles/creating-releases/
