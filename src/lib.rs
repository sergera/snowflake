use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/*

bit anatomy (i64):
_

1 bit: signing bit, should always be positive (zero)
_

44 bits: millisenconds since epoch
 - max of aprox 17592186044415 milliseconds, which is about 565 years
 - should work until the year 2535 CE using UNIX_EPOCH
_

17 bits: sequence, max of 131071
_

2 bits: service_id, max of 4 services
 - having the service id as the least significant bits means the snowflake id
 is roughly sortable by creation order
_

maximum of 131071 unique ids per service per millisecond
i.e. over 131 million unique ids per service per second
i.e. over 524 million unique ids per second using 4 services

*/

const MAX_17_BITS: u32 = 131071;
const MAX_2_BITS: u16 = 3;

#[derive(Debug)]
pub struct Snowflake {
    epoch: SystemTime,
    service_id: u16,
    last_millis: i64,
    seq: u32,
}

impl Snowflake {
    pub fn new(service_id: u16) -> Result<Self, SnowflakeError> {
        Ok(Self::with_epoch(service_id, UNIX_EPOCH)?)
    }

    pub fn with_epoch(service_id: u16, epoch: SystemTime) -> Result<Self, SnowflakeError> {
        if service_id > MAX_2_BITS {
            return Err(SnowflakeError::InvalidServiceIdError);
        }
        Ok(Self {
            epoch,
            service_id,
            last_millis: 0,
            seq: 0,
        })
    }

    pub fn gen(&mut self) -> i64 {
        let mut millis = self.get_time_millis();
        if self.seq == 0 && millis == self.last_millis {
            // if the sequence looped in the same millisecond, wait a millisecond
            sleep(Duration::from_millis(1));
            millis = self.get_time_millis();
        };
        self.last_millis = millis;
        millis << 19 | ((self.next_seq()) << 2) as i64 | self.service_id as i64
    }

    fn next_seq(&mut self) -> u32 {
        self.seq = (self.seq + 1) % MAX_17_BITS;
        self.seq
    }

    fn get_time_millis(&self) -> i64 {
        SystemTime::now()
            .duration_since(self.epoch)
            .unwrap()
            .as_millis() as i64
    }
}

#[derive(Debug)]
pub enum SnowflakeError {
    InvalidServiceIdError,
}

impl std::fmt::Display for SnowflakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::InvalidServiceIdError => write!(f, "service id must fit in 2 bits"),
        }
    }
}

impl std::error::Error for SnowflakeError {}

#[cfg(test)]
mod tests {
    use super::*;

    const NUM_IDS: u64 = 1_000_000;

    #[test]
    fn test_snowflake_creates_unique_positive_ids() {
        let mut snowflake = Snowflake::new(0).unwrap();
        let mut ids: Vec<i64> = Vec::new();
        for _ in 0..NUM_IDS {
            ids.push(snowflake.gen());
        }
        ids.sort();
        ids.dedup();
        ids = ids.into_iter().filter(|id| *id > 0).collect();
        assert_eq!(ids.len(), NUM_IDS as usize);
    }
}
