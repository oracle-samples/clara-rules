# How to Release

This project is hosted on [Clojars][clojars].  You can see it [here][release-site].

Releasing the project requires these steps:

0. Assert all tests are passing and the project builds : `lein do clean, test`
1. Make sure CHANGELOG.md is up-to-date for the upcoming release.
2. Assert you have Github setup with [gpg](https://docs.github.com/en/authentication/managing-commit-signature-verification/adding-a-gpg-key-to-your-github-account)
3. Add gpg key to [sign](https://git-scm.com/book/en/v2/Git-Tools-Signing-Your-Work) your commits
   * GPG will likely require an additional export to spawn an interactive prompt for signing:
     ```export GPG_TTY=$(tty)```
4. Create a [Clojars][clojars] Account and [Deploy Token](https://github.com/clojars/clojars-web/wiki/Deploy-Tokens) if you do not already have one.
5. Create a lein [credentials](https://leiningen.org/deploy.html#gpg) file using the account and token above.
6. Run `lein release <release-type>`, where release-type is one of `:patch`,`:minor` and `:major`
7. Push the new main branch to the repo.
8. Push the new tag to the repo.

[clojars]: https://clojars.org
[release-site]: https://clojars.org/com.cerner/clara-rules

