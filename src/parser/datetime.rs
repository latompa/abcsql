/// Days since 1970-01-01 (civil calendar algorithm)
pub fn date_to_epoch_days(y: i32, m: i32, d: i32) -> i32 {
    let (y, m) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub fn epoch_days_to_date(z: i32) -> (i32, i32, i32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365*yoe + yoe/4 - yoe/100);
    let mp = (5*doy + 2) / 153;
    let d = doy - (153*mp + 2)/5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Parse "YYYY-MM-DD" string into epoch days; returns None on parse failure
pub fn parse_date_str(s: &str) -> Option<i32> {
    let s = s.trim();
    let parts: Vec<&str> = s.splitn(3, '-').collect();
    if parts.len() != 3 { return None; }
    let y: i32 = parts[0].parse().ok()?;
    let m: i32 = parts[1].parse().ok()?;
    let d: i32 = parts[2].parse().ok()?;
    if m < 1 || m > 12 || d < 1 || d > 31 { return None; }
    Some(date_to_epoch_days(y, m, d))
}

/// Parse "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS" into epoch seconds
pub fn parse_timestamp_str(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date_part, time_part) = if let Some(p) = s.find(' ') {
        (&s[..p], &s[p+1..])
    } else if let Some(p) = s.find('T') {
        (&s[..p], &s[p+1..])
    } else {
        // date only → midnight
        return parse_date_str(s).map(|d| d as i64 * 86400);
    };
    let days = parse_date_str(date_part)? as i64;
    let tparts: Vec<&str> = time_part.splitn(3, ':').collect();
    let h: i64 = tparts.get(0).and_then(|s| s.parse().ok()).unwrap_or(0);
    let m: i64 = tparts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let sec_str = tparts.get(2).copied().unwrap_or("0");
    let sec_str = sec_str.split('.').next().unwrap_or("0");
    let sec: i64 = sec_str.parse().unwrap_or(0);
    Some(days * 86400 + h * 3600 + m * 60 + sec)
}

pub fn format_date(days: i32) -> String {
    let (y, m, d) = epoch_days_to_date(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

pub fn format_timestamp(secs: i64) -> String {
    // Handle negative timestamps carefully
    let days = if secs >= 0 { secs / 86400 } else { (secs - 86399) / 86400 };
    let time = secs - days * 86400;
    let (y, m, d) = epoch_days_to_date(days as i32);
    let h = time / 3600;
    let min = (time % 3600) / 60;
    let s = time % 60;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, min, s)
}

pub fn current_epoch_days() -> i32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    (secs / 86400) as i32
}

pub fn current_epoch_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

pub fn interval_unit_secs(unit: &str) -> Option<i64> {
    match unit.to_uppercase().as_str() {
        "SECOND" | "SECONDS" => Some(1),
        "MINUTE" | "MINUTES" => Some(60),
        "HOUR" | "HOURS" => Some(3600),
        "DAY" | "DAYS" => Some(86400),
        "WEEK" | "WEEKS" => Some(604800),
        "MONTH" | "MONTHS" => Some(2592000),   // approximate 30 days
        "YEAR" | "YEARS" => Some(31536000),    // approximate 365 days
        _ => None,
    }
}
