use crate::isolate::Isolate;

pub const TEST_JS: &str = include_str!("scripts/test.js");

#[test]
fn no_params_no_ret() {
    let mut isolate = Isolate::new();
    let ret: i32 = isolate
        .invoke("test.js", TEST_JS, "no_params_no_ret")
        .unwrap();

    assert_eq!(ret, 36);
}

#[test]
fn throws() {
    let mut isolate = Isolate::new();
    let err = isolate
        .invoke::<Option<i32>>("test.js", TEST_JS, "throws")
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        r#"exception in script: Uncaught: Error: test exception
 --> test.js:6:1-2
	throw new Error("test exception");
 ^


Stack trace:
Error: test exception
    at throws (test.js:6:8)
"#
    );
}
