use std::env;

use anyhow::Result;
use fs_err as fs;
use tempfile::TempDir;

use super::fixtures::{MockBinary, TempInstallDir};
use crate::DEFAULT_REPO;

#[tokio::test]
async fn init_creates_directory_structure() -> Result<()> {
    let temp = TempInstallDir::new()?;

    crate::commands::init::run(
        Some(temp.path().to_path_buf()),
        true, // no_modify_path
        true, // no_install_latest
        None, // github_token
    )
    .await?;

    assert!(temp.ampup_binary().exists(), "ampup binary not created");
    assert!(temp.bin_dir().exists(), "bin directory not created");
    assert!(
        temp.versions_dir().exists(),
        "versions directory not created"
    );

    Ok(())
}

#[tokio::test]
async fn init_fails_if_already_initialized() -> Result<()> {
    let temp = TempInstallDir::new()?;

    // First init should succeed
    crate::commands::init::run(Some(temp.path().to_path_buf()), true, true, None).await?;

    // Second init should fail
    let result =
        crate::commands::init::run(Some(temp.path().to_path_buf()), true, true, None).await;

    assert!(
        result.is_err(),
        "Expected init to fail when already initialized"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("already initialized")
    );

    Ok(())
}

#[tokio::test]
async fn list_shows_no_versions_when_empty() -> Result<()> {
    let temp = TempInstallDir::new()?;

    // Just verify it doesn't crash - actual output goes to stdout
    crate::commands::list::run(Some(temp.path().to_path_buf()))?;

    Ok(())
}

#[tokio::test]
async fn list_shows_installed_versions() -> Result<()> {
    let temp = TempInstallDir::new()?;

    // Create mock versions
    MockBinary::create(&temp, "v1.0.0")?;
    MockBinary::create(&temp, "v1.1.0")?;

    // Set current version
    fs::write(temp.current_version_file(), "v1.0.0")?;

    // Just verify it doesn't crash - actual output goes to stdout
    crate::commands::list::run(Some(temp.path().to_path_buf()))?;

    Ok(())
}

#[tokio::test]
async fn use_switches_to_installed_version() -> Result<()> {
    let temp = TempInstallDir::new()?;

    // Create mock versions first
    MockBinary::create(&temp, "v1.0.0")?;
    MockBinary::create(&temp, "v1.1.0")?;

    // Switch to v1.0.0
    crate::commands::use_version::run(Some(temp.path().to_path_buf()), Some("v1.0.0".to_string()))?;

    // Verify current version
    let current = fs::read_to_string(temp.current_version_file())?;
    assert_eq!(current.trim(), "v1.0.0");

    // Verify symlink points to correct binary
    let active_binary = temp.active_binary();
    assert!(active_binary.exists() || active_binary.is_symlink());

    // Switch to v1.1.0
    crate::commands::use_version::run(Some(temp.path().to_path_buf()), Some("v1.1.0".to_string()))?;

    let current = fs::read_to_string(temp.current_version_file())?;
    assert_eq!(current.trim(), "v1.1.0");

    Ok(())
}

#[tokio::test]
async fn use_fails_for_non_existent_version() -> Result<()> {
    let temp = TempInstallDir::new()?;

    let result = crate::commands::use_version::run(
        Some(temp.path().to_path_buf()),
        Some("v99.99.99".to_string()),
    );

    assert!(
        result.is_err(),
        "Expected use to fail for non-existent version"
    );
    assert!(result.unwrap_err().to_string().contains("not installed"));

    Ok(())
}

#[tokio::test]
async fn uninstall_removes_version() -> Result<()> {
    let temp = TempInstallDir::new()?;

    // Create mock versions first
    MockBinary::create(&temp, "v1.0.0")?;
    MockBinary::create(&temp, "v1.1.0")?;

    // Set current version to v1.1.0
    crate::commands::use_version::run(Some(temp.path().to_path_buf()), Some("v1.1.0".to_string()))?;

    // Uninstall v1.0.0 (not current)
    crate::commands::uninstall::run(Some(temp.path().to_path_buf()), "v1.0.0")?;

    assert!(
        !temp.version_dir("v1.0.0").exists(),
        "Version directory should be removed"
    );
    assert!(
        temp.version_dir("v1.1.0").exists(),
        "Other version should still exist"
    );

    Ok(())
}

#[tokio::test]
async fn uninstall_fails_for_non_existent_version() -> Result<()> {
    let temp = TempInstallDir::new()?;

    let result = crate::commands::uninstall::run(Some(temp.path().to_path_buf()), "v99.99.99");

    assert!(
        result.is_err(),
        "Expected uninstall to fail for non-existent version"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "Re-enable this and bump versions once the repository is public"]
async fn install_latest_version() -> Result<()> {
    let temp = TempInstallDir::new()?;

    // Install latest version
    crate::commands::install::run(
        Some(temp.path().to_path_buf()),
        DEFAULT_REPO.to_string(),
        None,
        None,
        None,
        None,
        None,
    )
    .await?;

    // Verify a version was installed
    let versions: Vec<_> = fs::read_dir(temp.versions_dir())?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().ok().map(|ft| ft.is_dir()).unwrap_or(false))
        .collect();

    assert!(!versions.is_empty(), "No version was installed");

    Ok(())
}

#[tokio::test]
#[ignore = "Re-enable this and bump versions once the repository is public"]
async fn install_specific_version() -> Result<()> {
    let temp = TempInstallDir::new()?;

    // Install a specific version (use a known release)
    let version = "v0.0.21";
    crate::commands::install::run(
        Some(temp.path().to_path_buf()),
        DEFAULT_REPO.to_string(),
        None,
        Some(version.to_string()),
        None,
        None,
        None,
    )
    .await?;

    assert!(
        temp.version_dir(version).exists(),
        "Version directory not created"
    );
    assert!(temp.version_binary(version).exists(), "Binary not created");

    Ok(())
}

#[tokio::test]
#[ignore = "Re-enable this and bump versions once the repository is public"]
async fn install_already_installed_version_switches_to_it() -> Result<()> {
    let temp = TempInstallDir::new()?;
    let version = "v0.0.21";

    // Install once
    crate::commands::install::run(
        Some(temp.path().to_path_buf()),
        DEFAULT_REPO.to_string(),
        None,
        Some(version.to_string()),
        None,
        None,
        None,
    )
    .await?;

    // Install again - should just switch to it
    crate::commands::install::run(
        Some(temp.path().to_path_buf()),
        DEFAULT_REPO.to_string(),
        None,
        Some(version.to_string()),
        None,
        None,
        None,
    )
    .await?;

    let current = fs::read_to_string(temp.current_version_file())?;
    assert_eq!(current.trim(), version);

    Ok(())
}

#[tokio::test]
async fn build_from_local_path_with_custom_name() -> Result<()> {
    let temp = TempInstallDir::new()?;

    // Create a temporary directory that looks like a amp repo
    let fake_repo = TempDir::new()?;
    let target_dir = fake_repo.path().join("target/release");
    fs::create_dir_all(&target_dir)?;

    // Create mock ampd binary
    let mock_ampd = target_dir.join("ampd");
    fs::write(&mock_ampd, "#!/bin/sh\necho 'ampd test-version'")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&mock_ampd)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&mock_ampd, perms)?;
    }

    // Create mock ampctl binary
    let mock_ampctl = target_dir.join("ampctl");
    fs::write(&mock_ampctl, "#!/bin/sh\necho 'ampctl test-version'")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&mock_ampctl)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&mock_ampctl, perms)?;
    }

    // Mock cargo by creating a fake cargo script
    let mock_cargo_dir = TempDir::new()?;
    let mock_cargo = mock_cargo_dir.path().join("cargo");
    fs::write(&mock_cargo, "#!/bin/sh\nexit 0")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&mock_cargo)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&mock_cargo, perms)?;
    }

    // Temporarily modify PATH to use mock cargo
    let original_path = env::var("PATH").unwrap_or_default();
    let new_path = format!("{}:{}", mock_cargo_dir.path().display(), original_path);
    unsafe {
        env::set_var("PATH", &new_path);
    }

    let custom_name = "my-custom-build";
    let result = crate::commands::build::run(
        Some(temp.path().to_path_buf()),
        None, // repo
        Some(fake_repo.path().to_path_buf()),
        None, // branch
        None, // commit
        None, // pr
        Some(custom_name.to_string()),
        None, // jobs
    )
    .await;

    // Restore PATH
    unsafe {
        env::set_var("PATH", &original_path);
    }

    result?;

    assert!(
        temp.version_dir(custom_name).exists(),
        "Custom version not created"
    );
    assert!(
        temp.version_binary(custom_name).exists(),
        "Binary not installed"
    );

    Ok(())
}
