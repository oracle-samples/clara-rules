# How to Release

This project is hosted on [Clojars][clojars].  You can see it [here][release-site].

Releasing the project requires these steps:

0. Set the version number in the project.clj file.
1. Run ```lein do clean, test``` to ensure everything is working as expected.
2. Use a GitHub [project release][github-release-url] to release the project and tag (be sure it follows [semver][semantic-versioning])
3. Run ```lein deploy clojars``` to deploy the project to the Clojars repository.
4. Update `main` to a new minor version

[clojars]: https://clojars.org
[release-site]: https://clojars.org/com.cerner/clara-rules
[project-url]: https://github.com/cerner/clara-rules/
[semantic-versioning]: http://semver.org/
[github-release-url]: https://help.github.com/articles/creating-releases/
