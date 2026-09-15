# Core distribution

Release archives contain the `kakune` executable, both project READMEs, the license, examples, and service templates. Verify `SHA256SUMS` before extracting an archive. GitHub release provenance can be verified with `gh attestation verify <asset> --repo BraveOtter/kakune-core`.

Extract the archive to a directory owned by the user or administrator and run `kakune init --data-dir <directory>`. The `packaging/linux/kakune-core.service` and `packaging/macos/dev.kakune.core.plist` files are templates: update their executable and data paths before installing a service.

Windows installers and macOS notarization are published only after their respective signing credentials are configured. An unsigned archive is always labelled unsigned.
