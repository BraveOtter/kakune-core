//! All task children belong to an OS process group or Windows Job Object.
use std::io;

pub fn spawn_async(
    command: tokio::process::Command,
) -> io::Result<Box<dyn process_wrap::tokio::ChildWrapper>> {
    use process_wrap::tokio::*;
    let mut command = CommandWrap::from(command);
    command.wrap(KillOnDrop);
    #[cfg(unix)]
    command.wrap(ProcessGroup::leader());
    #[cfg(windows)]
    {
        command.wrap(CreationFlags(
            windows::Win32::System::Threading::CREATE_NO_WINDOW,
        ));
        command.wrap(JobObject);
    }
    command.spawn()
}

pub fn spawn_sync(
    command: std::process::Command,
) -> io::Result<Box<dyn process_wrap::std::ChildWrapper>> {
    use process_wrap::std::*;
    let mut command = CommandWrap::from(command);
    #[cfg(unix)]
    command.wrap(ProcessGroup::leader());
    #[cfg(windows)]
    {
        command.wrap(CreationFlags(
            windows::Win32::System::Threading::CREATE_NO_WINDOW,
        ));
        command.wrap(JobObject);
    }
    command.spawn()
}
