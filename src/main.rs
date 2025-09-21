use anyhow::{Result, anyhow};
use nix::libc::{MAP_FAILED, MAP_SHARED, PROT_READ, mlock, mmap64, munmap};
use smol::{
    block_on,
    fs::{self, read_dir},
    lock::Mutex,
    process::Command,
    stream::StreamExt,
};
use std::{
    env,
    ffi::{OsStr, c_void},
    ops::{AddAssign, DerefMut},
    os::fd::{AsFd, AsRawFd},
    path::{Path, PathBuf},
    process::{self, ExitStatus, Stdio},
    ptr::null_mut,
    sync::Arc,
};
use sysinfo::System;
const STEAM_APPS: &str = "steamapps";
const COMMON: &str = "common";
const COMPATDATA: &str = "compatdata";
const SHADERCACHE: &str = "shadercache";

const MIN_KEEP_MEM_SIZE: u64 = 4 * 1024 * 1024 * 1024;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() > 2 {
        let exit_status = block_on(run(args))?;
        if let Some(code) = exit_status.code() {
            process::exit(code);
        } else {
            Err(anyhow!("Unknown exit status."))
        }
    } else {
        Err(anyhow!("Args is not enough."))
    }
}

async fn run(args: Vec<String>) -> Result<ExitStatus> {
    let command = args[1].to_owned();
    let args = args[2..].to_owned();
    let (rc_result, pl_result) = smol::future::zip(
        run_command(command.clone(), args.clone()),
        pre_load_files(args),
    )
    .await;
    let status = rc_result?;
    drops(pl_result?);
    Ok(status)
}
fn drops(mems: Vec<MappedMem>) {
    for mut mem in mems {
        mem.release();
    }
}

async fn run_command(command: String, args: Vec<String>) -> Result<ExitStatus> {
    let status = Command::new(command)
        .args(args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await?;
    Ok(status)
}

async fn pre_load_files(args: Vec<String>) -> Result<Vec<MappedMem>> {
    let steam_game =
        ditect_steam_game(&args).ok_or(anyhow!("Can not find steamapss directory."))?;
    let app_id = ditect_app_id(&args).ok_or(anyhow!("Can not find steam app id."))?;
    let in_proton = if let Some(ext) = steam_game.exefile_path.extension().and_then(OsStr::to_str)
        && ext == "exe"
    {
        Some((
            steam_game.common_dir.join("Steam.dll"),
            steam_game.steamapps_dir.join(COMPATDATA).join(&app_id),
        ))
    } else {
        None
    };
    let mut load_files_and_dirs = vec![
        steam_game.game_dir,
        steam_game.steamapps_dir.join(SHADERCACHE).join(&app_id),
    ];
    if let Some((steam_dll_path, proton_env_dir)) = in_proton {
        load_files_and_dirs.push(steam_dll_path);
        load_files_and_dirs.push(proton_env_dir);
    }
    println!("pre load files:");
    println!("{load_files_and_dirs:?}");
    let mut sys = System::new_all();
    sys.refresh_all();
    let cached_mem_size = Arc::new(Mutex::new(0));
    load_file_paths(load_files_and_dirs, &sys, cached_mem_size).await
}

struct SteamGame {
    exefile_path: PathBuf,
    steamapps_dir: PathBuf,
    game_dir: PathBuf,
    common_dir: PathBuf,
}

fn ditect_steam_game(args: &[String]) -> Option<SteamGame> {
    for arg in args {
        if arg.contains(STEAM_APPS) {
            let target_path = Path::new(arg);
            for t in target_path.ancestors() {
                if let Some(common_target) = t.parent()
                    && common_target.file_name().unwrap().to_str().unwrap() == COMMON
                    && let Some(steam_target) = common_target.parent()
                {
                    return Some(SteamGame {
                        exefile_path: target_path.to_path_buf(),
                        steamapps_dir: steam_target.to_path_buf(),
                        game_dir: t.to_path_buf(),
                        common_dir: common_target.to_path_buf(),
                    });
                }
            }
        }
    }
    None
}
const APP_ID_PREFIX: &str = "AppId=";
fn ditect_app_id(args: &[String]) -> Option<String> {
    for arg in args {
        if arg.contains(APP_ID_PREFIX) {
            let app_id = arg.replace(APP_ID_PREFIX, "");
            return Some(app_id.trim().to_owned());
        }
    }
    None
}

struct MappedMem {
    addr: *mut c_void,
    len: usize,
}
impl MappedMem {
    fn new(addr: *mut c_void, len: usize) -> Self {
        Self { addr, len }
    }
    fn release(&mut self) {
        unsafe { munmap(self.addr, self.len) };
    }
}

async fn load_file_paths(
    file_and_dirs: Vec<PathBuf>,
    sys: &System,
    cached_mem_size: Arc<Mutex<u64>>,
) -> Result<Vec<MappedMem>> {
    let mut tasks = vec![];
    for file_or_dir in file_and_dirs {
        tasks.push(Box::pin(load_path(
            file_or_dir,
            sys,
            cached_mem_size.clone(),
        )));
    }
    let mut mms = vec![];
    for task in tasks {
        mms.append(&mut task.await?);
    }
    Ok(mms)
}

async fn load_path(
    path: impl AsRef<Path>,
    sys: &System,
    cached_mem_size: Arc<Mutex<u64>>,
) -> Result<Vec<MappedMem>> {
    let path = path.as_ref();
    if path.exists() {
        if path.is_file() {
            if let Some(mmap) = load_file(path, sys, cached_mem_size).await? {
                Ok(vec![mmap])
            } else {
                Ok(vec![])
            }
        } else if path.is_dir() {
            Ok(load_dir(path, sys, cached_mem_size).await?)
        } else {
            Err(anyhow!("unknown path."))
        }
    } else {
        // There is a possibility that the file may be deleted due to other reasons.
        Ok(vec![])
    }
}

async fn load_file(
    file_path: impl AsRef<Path>,
    sys: &System,
    cached_mem_size: Arc<Mutex<u64>>,
) -> Result<Option<MappedMem>> {
    let file_size = fs::metadata(&file_path).await?.len() as usize;
    let need_mlock = {
        let mut cms = cached_mem_size.lock().await;
        let lock_size = file_size as u64;
        let free_mem = sys.free_memory() - lock_size;
        if free_mem > MIN_KEEP_MEM_SIZE {
            cms.deref_mut().add_assign(lock_size);
            true
        } else {
            false
        }
    };
    if need_mlock {
        let file = fs::File::open(&file_path).await?;
        let fd = file.as_fd();

        unsafe {
            let mem = mmap64(
                null_mut(),
                file_size,
                PROT_READ,
                MAP_SHARED,
                fd.as_raw_fd(),
                0,
            );
            if mem != MAP_FAILED {
                mlock(mem, file_size);
                Ok(Some(MappedMem::new(mem, file_size)))
            } else {
                Ok(None)
            }
        }
    } else {
        Ok(None)
    }
}
async fn load_dir(
    dir_path: impl AsRef<Path>,
    sys: &System,
    cached_mem_size: Arc<Mutex<u64>>,
) -> Result<Vec<MappedMem>> {
    let mut paths = vec![];
    let mut entries = read_dir(dir_path).await?;
    while let Some(entry) = entries.try_next().await? {
        paths.push(entry.path());
    }
    load_file_paths(paths, sys, cached_mem_size).await
}
