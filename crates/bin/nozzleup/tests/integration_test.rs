use std::env;

#[test]
fn test_platform_detection() {
    // This test verifies that platform detection works on the current system
    // It should pass on any supported platform (linux, darwin)
    let os = env::consts::OS;
    assert!(
        os == "linux" || os == "macos",
        "Platform {} should be supported",
        os
    );
}

#[test]
fn test_arch_detection() {
    // This test verifies that architecture detection works on the current system
    // It should pass on any supported architecture (x86_64, aarch64)
    let arch = env::consts::ARCH;
    assert!(
        arch == "x86_64" || arch == "aarch64" || arch == "amd64" || arch == "arm64",
        "Architecture {} should be supported",
        arch
    );
}
