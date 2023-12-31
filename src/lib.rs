use std::sync::{Arc, Mutex};
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

pub struct ConcurrentSnowflake {
    inner: Arc<Mutex<Snowflake>>,
}

impl ConcurrentSnowflake {
    pub fn new(service_id: u16) -> Result<Self, SnowflakeError> {
        Ok(Self {
            inner: Arc::new(Mutex::new(Snowflake::with_epoch(service_id, UNIX_EPOCH)?)),
        })
    }

    pub fn with_epoch(service_id: u16, epoch: SystemTime) -> Result<Self, SnowflakeError> {
        Ok(Self {
            inner: Arc::new(Mutex::new(Snowflake::with_epoch(service_id, epoch)?)),
        })
    }

    pub fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn gen(&mut self) -> Result<i64, ConcurrentSnowflakeError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| ConcurrentSnowflakeError::PoisonError)?
            .gen())
    }
}

#[derive(Debug)]
pub enum ConcurrentSnowflakeError {
    PoisonError,
    SnowflakeError(SnowflakeError),
}

impl std::fmt::Display for ConcurrentSnowflakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::PoisonError => write!(
                f,
                "lock was poisoned during a previous access and can no longer be locked"
            ),
            Self::SnowflakeError(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for ConcurrentSnowflakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SnowflakeError(e) => Some(e),
            _ => None,
        }
    }
}

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
        let (current_time, mut millis) = self.get_time();

        if millis > self.last_millis {
            // new millisecond, reset sequence
            self.seq = 0;
        } else if self.seq == MAX_17_BITS {
            // sequence was exhausted in the same millisecond, wait until next millisecond
            let elapsed_micros = current_time
                .duration_since(self.epoch)
                .unwrap()
                .subsec_micros();
            let sleep_duration = Duration::from_micros((1_000 - elapsed_micros) as u64);
            sleep(sleep_duration);
            millis += 1;
        }

        self.last_millis = millis;
        millis << 19 | ((self.next_seq()) << 2) as i64 | self.service_id as i64
    }

    fn next_seq(&mut self) -> u32 {
        self.seq = (self.seq + 1) % MAX_17_BITS;
        self.seq
    }

    fn get_time(&self) -> (SystemTime, i64) {
        let current_time = SystemTime::now();
        let millis = current_time.duration_since(self.epoch).unwrap().as_millis() as i64;
        (current_time, millis)
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

    #[test]
    fn test_snowflake_concurrently_creates_unique_positive_ids() {
        use std::thread::spawn;

        let snowflake = ConcurrentSnowflake::new(0).unwrap();

        let mut clone1 = snowflake.clone();
        let ids_thread_one = spawn(move || {
            let mut ids: Vec<i64> = Vec::new();
            for _ in 0..NUM_IDS {
                ids.push(clone1.gen().unwrap());
            }
            ids
        });

        let mut clone2 = snowflake.clone();
        let ids_thread_two = spawn(move || {
            let mut ids: Vec<i64> = Vec::new();
            for _ in 0..NUM_IDS {
                ids.push(clone2.gen().unwrap());
            }
            ids
        });

        let mut clone3 = snowflake.clone();
        let ids_thread_three = spawn(move || {
            let mut ids: Vec<i64> = Vec::new();
            for _ in 0..NUM_IDS {
                ids.push(clone3.gen().unwrap());
            }
            ids
        });

        let mut clone4 = snowflake.clone();
        let ids_thread_four = spawn(move || {
            let mut ids: Vec<i64> = Vec::new();
            for _ in 0..NUM_IDS {
                ids.push(clone4.gen().unwrap());
            }
            ids
        });

        let mut ids: Vec<i64> = Vec::new();
        ids.extend(ids_thread_one.join().unwrap());
        ids.extend(ids_thread_two.join().unwrap());
        ids.extend(ids_thread_three.join().unwrap());
        ids.extend(ids_thread_four.join().unwrap());

        ids.sort();
        ids.dedup();
        ids = ids.into_iter().filter(|id| *id > 0).collect();
        assert_eq!(ids.len(), (NUM_IDS * 4) as usize);
    }
}
