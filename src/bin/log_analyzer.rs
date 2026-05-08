use anyhow::Result;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

use llmproxy::binlib::log_analyzer;

fn main() -> Result<()> {
    let log_path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: log_analyzer <log_file>");
        std::process::exit(1);
    });

    let output_path = output_path_for(&log_path);

    let file = File::open(&log_path)?;
    let lines: Vec<String> = BufReader::new(file)
        .lines()
        .collect::<std::io::Result<_>>()?;

    // 保持原实现的语义：解析失败的 timestamp 回退到当前时间
    let stats = log_analyzer::analyze_lines(lines, chrono::Utc::now());

    let generated_at = generated_at_now();
    let markdown = log_analyzer::render_markdown(&log_path, &generated_at, &stats);

    let mut out = File::create(&output_path)?;
    out.write_all(markdown.as_bytes())?;

    eprintln!("✅ 分析报告已生成: {}", output_path);
    Ok(())
}

fn output_path_for(log_path: &str) -> String {
    format!("{log_path}.md")
}

fn generated_at_now() -> String {
    chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_path_appends_markdown_suffix() {
        assert_eq!(output_path_for("logs/proxy.log"), "logs/proxy.log.md");
    }

    #[test]
    fn generated_at_uses_expected_display_shape() {
        let value = generated_at_now();
        assert_eq!(value.len(), "2026-01-01 00:00:00".len());
        assert_eq!(&value[4..5], "-");
        assert_eq!(&value[7..8], "-");
        assert_eq!(&value[10..11], " ");
        assert_eq!(&value[13..14], ":");
        assert_eq!(&value[16..17], ":");
    }
}
