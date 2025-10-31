use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // use std::{env, path::PathBuf, process::Command};

    // unsafe{env::set_var("MACOSX_DEPLOYMENT_TARGET", "15.5")};
    // unsafe{env::set_var("GOARCH", "arm64")};
    // unsafe{env::set_var("GOOS", "darwin")};
    // unsafe{env::set_var("CGO_ENABLED", "1")};
    // unsafe{env::set_var("GOHOSTARCH", "arm64")};
    // let in_dir = PathBuf::from("/tmp/arrow-adbc/build");
    // let status = Command::new("cmake")
    //     .current_dir(in_dir.as_path())
    //     .arg("../c")
    //     .arg("-DADBC_DRIVER_SNOWFLAKE=ON")
    //     .arg("-DADBC_DRIVER_MANAGER=ON")
    //     .arg("-DADBC_DRIVER_POSTGRESQL=ON")
    //     .arg("-DADBC_DRIVER_BIGQUERY=ON")
    //     .arg("-DADBC_DRIVER_SQLITE=OFF")
    //     .status()?;
    // assert!(status.success(), "CMake configuration failed");
    // let status = Command::new("make")
    //     .current_dir(in_dir.as_path())
    //     .arg("-j")
    //     .status()?;
    // assert!(status.success(), "CMake build failed");

    Ok(())
}
