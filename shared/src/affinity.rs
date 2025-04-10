use std::{
    collections::HashSet,
    io,
    mem::{size_of, zeroed},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[cfg(target_os = "linux")]
pub fn get_thread_affinity() -> Result<Option<HashSet<usize>>> {
    use libc::{cpu_set_t, sched_getaffinity, CPU_ISSET, CPU_SETSIZE};

    let mut affinity = HashSet::new();
    let mut set: cpu_set_t = unsafe { zeroed() };

    let res = unsafe { sched_getaffinity(0, size_of::<cpu_set_t>(), &mut set) };
    if res != 0 {
        return Err(From::from(format!(
            "sched_getaffinity failed with errno {}",
            io::Error::from(errno::errno())
        )));
    }

    for i in 0..CPU_SETSIZE as usize {
        if unsafe { CPU_ISSET(i, &set) } {
            affinity.insert(i);
        }
    }

    Ok(Some(affinity))
}

#[cfg(not(target_os = "linux"))]
pub fn get_thread_affinity() -> Result<Option<HashSet<usize>>> {
    None
}

#[cfg(target_os = "linux")]
pub fn set_thread_affinity(core_ids: impl Iterator<Item = usize>) -> Result<()> {
    use libc::{cpu_set_t, sched_setaffinity, CPU_SET};

    let mut set: cpu_set_t = unsafe { zeroed() };
    unsafe {
        for core_id in core_ids {
            CPU_SET(core_id, &mut set);
        }
    }

    let res = unsafe { sched_setaffinity(0, size_of::<cpu_set_t>(), &set) };
    if res != 0 {
        return Err(From::from(format!(
            "sched_setaffinity failed with errno {}",
            io::Error::from(errno::errno())
        )));
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_thread_affinity(core_ids: impl Iterator<Item = usize>) -> Result<()> {
    None
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{get_thread_affinity, set_thread_affinity};

    #[test]
    fn get() {
        let cpus = get_thread_affinity();
        assert!(matches!(cpus, Ok(Some(_))), "produce set");
        let set = cpus.unwrap().unwrap();
        assert!(!set.is_empty(), "not empty");
    }

    #[test]
    fn set() {
        assert!(set_thread_affinity([0].into_iter()).is_ok());
    }
}
