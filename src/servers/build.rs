// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

fn main() {
    #[cfg(feature = "dashboard")]
    fetch_dashboard_assets();
}

#[cfg(feature = "dashboard")]
fn fetch_dashboard_assets() {
    use std::process::{Command, Stdio};

    macro_rules! p {
        ($($tokens: tt)*) => {
            println!("cargo:warning={}", format!($($tokens)*))
        }
    }

    let output = Command::new("./fetch-dashboard-assets.sh")
        .current_dir("../../scripts")
        .stdout(Stdio::piped())
        .spawn()
        .and_then(|p| p.wait_with_output());
    match output {
        Ok(output) => {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .for_each(|x| p!("{}", x));

            assert!(output.status.success());
        }
        Err(e) => {
            let e = format!(
                r#"
Failed to fetch dashboard assets: {}. 
You can manually execute './scripts/fetch-dashboard-assets.sh' to see why, 
or it's a network error, just try again or enable/disable some proxy."#,
                e
            );
            panic!("{}", e);
        }
    }
}
