# Fork alpha releases

The `Fork Alpha Release` workflow builds a signed macOS arm64 prerelease in
`Dede98/t3code`. The packaged updater configuration is derived from the workflow repository, so
installed fork builds read future prereleases from `Dede98/t3code` rather than `pingdotgg/t3code`.

## Configure signing secrets

1. In Keychain Access, open **My Certificates**, select the Apple Development certificate together
   with its private key, and export it as a password-protected `.p12` file.
2. Encode the export without adding it to the repository:

   ```bash
   base64 -i /path/to/apple-development.p12 | pbcopy
   ```

3. In the fork, open **Settings → Secrets and variables → Actions** and add these repository
   secrets:

   - `MACOS_CERTIFICATE_P12`: the Base64 text copied above.
   - `MACOS_CERTIFICATE_PASSWORD`: the `.p12` export password.

4. Delete the local `.p12` export after the secrets are configured.

The certificate, private key, password, signing identity, team ID, and certificate fingerprint must
not be committed. The workflow imports the certificate into a temporary keychain and deletes that
keychain after the run.

## Publish an alpha

Open **Actions → Fork Alpha Release → Run workflow**. A successful run creates a GitHub prerelease
containing the signed DMG, update ZIP, blockmaps, and `nightly-mac.yml`.

The first fork build must be installed manually from its DMG. Existing upstream builds keep their
embedded `pingdotgg/t3code` update feed; installing the first fork build replaces that embedded feed.
Subsequent fork builds can then be discovered and installed by the desktop updater.
