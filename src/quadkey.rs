use std::f64::consts::PI;

#[derive(Debug)]
pub struct Quadkey(String);

impl Quadkey {
    pub fn new(lat: f64, lon: f64, zoom: u8) -> Self {
        let (xtile, ytile) = Self::tile(lat, lon, zoom);
        let mut quad_key = String::new();
        for i in (1..=zoom).rev() {
            let mut digit: u8 = 0;
            let mask = 1 << (i - 1);
            if (xtile & mask) != 0 {
                digit += 1;
            }
            if (ytile & mask) != 0 {
                digit += 2;
            }
            let c = match digit {
                0 => '0',
                1 => '1',
                2 => '2',
                3 => '3',
                _ => '?',
            };
            quad_key.push(c);
        }
        return Self(quad_key);
    }

    pub fn to_string(&self) -> &String {
        &self.0
    }

    fn tile(lat: f64, lon: f64, zoom: u8) -> (u32, u32) {
        let lat_rad = lat * PI / 180.0;
        let n = (2.0 as f64).powi(zoom as i32);
        let xtile = ((lon + 180.0) / 360.0 * n).round() as u32;
        let ytile =
            ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).log2() / PI) / 2.0 * n).round() as u32;
        return (xtile, ytile);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_foo() {
        assert_eq!(Quadkey::new(0.0, 0.0, 4).to_string(), &"3000".to_string());
    }
}
