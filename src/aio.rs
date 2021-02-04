pub use aio_bindings::{
    io_context_t,
    iocb,
    io_event,
    timespec,
};

pub struct AlignedBuffer {
    pub ptr: *mut u8,
    size: usize,
    align: usize,
}

impl AlignedBuffer {
    pub fn new(size: usize, align: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(size, align).unwrap();
        let ptr = unsafe {
            std::alloc::alloc(layout)
        };
        AlignedBuffer {
            ptr, size, align
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) { 
        let layout = std::alloc::Layout::from_size_align(self.size, self.align).unwrap();
        unsafe {
            std::alloc::dealloc(self.ptr, layout)
        }
    }
}

pub struct AioContext {
    context: io_context_t,
}

impl AioContext {
    pub fn new() -> Self {
        let mut context: io_context_t = 0 as io_context_t;
        unsafe {
            let rc = aio_bindings::io_setup(32, &mut context as *mut io_context_t);
            assert_eq!(rc, 0);
        }
        AioContext { context }
    }

    pub fn submit(&self, request: &AioRequest) {
        let mut real_iocb: iocb = unsafe { std::mem::zeroed() };

        real_iocb.aio_lio_opcode = if request.write {
            aio_bindings::io_iocb_cmd_IO_CMD_PWRITE
        } else {
            aio_bindings::io_iocb_cmd_IO_CMD_PREAD
        } as std::os::raw::c_short;
        
        real_iocb.aio_fildes = request.fd;
        real_iocb.aio_reqprio = 0;
        real_iocb.u.c.buf = request.buf as *mut std::os::raw::c_void;
        real_iocb.u.c.nbytes = request.length as std::os::raw::c_ulong;
        real_iocb.u.c.offset = request.offset as std::os::raw::c_longlong;
        real_iocb.data = request.data;

        let mut iocb_arr: [*mut aio_bindings::iocb; 1] =
            [&mut real_iocb as *mut aio_bindings::iocb; 1];
        
        let rc = unsafe {
            aio_bindings::io_submit(self.context, 1, &mut iocb_arr[0] as *mut *mut aio_bindings::iocb)
        } as i32;
        if rc < 0 {
            panic!("io submit failed with {}", rc as i32);
        }
    }

    pub fn getevents(&self, us: i32) -> Option<AioEvent> {
        let mut event: aio_bindings::io_event = unsafe { std::mem::zeroed() };
        let mut wait = aio_bindings::timespec{ tv_sec: 0, tv_nsec: (us * 1000) as std::os::raw::c_long };

        let cnt = unsafe {
            aio_bindings::io_getevents(
                self.context, 1, 1, &mut event as *mut aio_bindings::io_event, &mut wait as *mut aio_bindings::timespec)
        };
        if cnt > 0 {
            Option::Some(AioEvent::new(event))
        } else if cnt == 0 {
            Option::None
        } else {
            panic!("Failed with ret {}", cnt);
        }
    }
}

impl Drop for AioContext {
    fn drop(&mut self) {
        unsafe {
            aio_bindings::io_destroy(self.context);
        }
    }
}

pub struct AioRequest {
    fd: i32,
    write: bool,
    buf: *mut u8,
    length: u64,
    offset: i64,
    data: *mut std::os::raw::c_void,
}

impl AioRequest {
    pub fn new_read(
        fd: i32,
        buf: *mut u8,
        length: u64,
        offset: i64,
        data: *mut std::os::raw::c_void,
    ) -> Self {
        let write = false;
        AioRequest {
            fd, write, buf, length, offset, data
        }
    }

    pub fn new_write(
        fd: i32,
        buf: *mut u8,
        length: u64,
        offset: i64,
        data: *mut std::os::raw::c_void,
    ) -> Self {
        let write = true;
        AioRequest {
            fd, write, buf, length, offset, data
        }
    } 
}

pub struct AioEvent {
    pub ret: i64,
    pub data: *mut std::os::raw::c_void,
}

impl AioEvent {
    pub fn new(event: aio_bindings::io_event) -> Self {
        AioEvent {
            ret: event.res as i64,
            data: event.data,
        }
    }
}
