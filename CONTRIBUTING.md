# Contributing to this repository

Help us to make this project better by contributing. Whether it's new features, bug fixes, or simply improving documentation, your contributions are welcome. Please start with logging a [github issue][1] or submit a pull request.

Before you contribute, please review these guidelines to help ensure a smooth process for everyone.

Thanks.

## Opening issues

For bugs or enhancement requests, please file a GitHub issue unless it's
security related. When filing a bug remember that the better written the bug is,
the more likely it is to be fixed. If you think you've found a security
vulnerability, do not raise a GitHub issue and follow the instructions in our
[security policy](./SECURITY.md).

* Please browse our [existing issues][1] before logging new issues.
* Check that the issue has not already been fixed in the `main` branch.
* Open an issue with a descriptive title and a summary.
* Please be as clear and explicit as you can in your description of the problem.
* Please state the version of Clojure and Clara you are using in the description.
* Include any relevant code in the issue summary.

## Contributing code

We welcome your code contributions. Before submitting code via a pull request,
you will need to have signed the [Oracle Contributor Agreement][OCA] (OCA) and
your commits need to include the following line using the name and e-mail
address you used to sign the OCA:

```text
Signed-off-by: Your Name <you@example.org>
```

This can be automatically added to pull requests by committing with `--sign-off`
or `-s`, e.g.

```text
git commit --signoff
```

Only pull requests from committers that can be verified as having signed the OCA
can be accepted.

## Pull request process

* Read [how to properly contribute to open source projects on Github][2].
* Fork the project.
* Use a feature branch.
* Write [good commit messages][3].
* Use the same coding conventions as the rest of the project.
* Commit locally and push to your fork until you are happy with your contribution.
* Make sure to add tests and verify all the tests are passing when merging upstream.
* Add an entry to the [Changelog][4] accordingly.
* Please add your name to the CONTRIBUTORS.md file. Adding your name to the CONTRIBUTORS.md file signifies agreement to all rights and reservations provided by the [License][5].
* [Squash related commits together][6].
* Open a [pull request][7].
* The pull request will be reviewed by the community and merged by the project committers.

## Code of conduct

Follow the [Golden Rule](https://en.wikipedia.org/wiki/Golden_Rule). If you'd
like more specific guidelines, see the [Contributor Covenant Code of Conduct][COC].

[1]: https://github.com/cerner/clara-rules/issues
[2]: http://gun.io/blog/how-to-github-fork-branch-and-pull-request
[3]: http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html
[4]: ./CHANGELOG.md
[5]: ./LICENSE
[6]: http://gitready.com/advanced/2009/02/10/squashing-commits-with-rebase.html
[7]: https://help.github.com/articles/using-pull-requests
[OCA]: https://oca.opensource.oracle.com
[COC]: https://www.contributor-covenant.org/version/1/4/code-of-conduct/
