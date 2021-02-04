mod aio;

use std::rc::Rc;
use std::task::*;
use std::future::*;
use aio::*;
use std::os::unix::io::AsRawFd;
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};

struct AioFuture {
    rc: Option<i64>,
    waker: Option<Waker>,
}

impl AioFuture {
    fn new() -> Self {
        AioFuture {
            rc: None,
            waker: None,
        }
    }
}

impl Future for AioFuture {
    type Output = i64;
    fn poll(self: std::pin::Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(rc) = self.rc {
            return Poll::Ready(rc);
        } else {
            self.get_mut().waker = Some(ctx.waker().clone());

            Poll::Pending
        }
    }
}

async fn async_read(
    ctx: &AioContext, fd: i32, buf: *mut u8, length: u64, offset: i64
) -> i64 {
    let future = AioFuture::new();
    futures::pin_mut!(future);

    let request = AioRequest::new_read(fd, buf, length, offset, 
         &*future as *const _ as *mut std::os::raw::c_void);
    ctx.submit(&request);
    
    future.await
}

async fn async_write(
    ctx: &AioContext, fd: i32, buf: *mut u8, length: u64, offset: i64
) -> i64 {
    let future = AioFuture::new();
    futures::pin_mut!(future);
    
    let request = AioRequest::new_write(fd, buf, length, offset, 
        &*future as *const _ as *mut std::os::raw::c_void );
    ctx.submit(&request);
    
    future.await
}

async fn async_copy(src_path: &str, dst_path: &str) {
    const O_DIRECT: i32 = 0o0040000;
    let src = OpenOptions::new().read(true).custom_flags(O_DIRECT).open(src_path).unwrap();
    let dst = OpenOptions::new().create(true).write(true).custom_flags(O_DIRECT).open(dst_path).unwrap();
    
    let src_fd = src.as_raw_fd() as i32;
    let dst_fd = dst.as_raw_fd() as i32;

    let aio_ctx = Rc::new(AioContext::new());

    // check event in background
    let aio_ctx2 = aio_ctx.clone();
    tokio::task::spawn_local(async move {
        let aio_ctx = &aio_ctx2;
        while Rc::strong_count(&aio_ctx) > 1 {
            if let Option::Some(event) = aio_ctx.getevents(10) {
                let future = unsafe { &mut *(event.data as *mut AioFuture) };
                future.rc = Some(event.ret);
                if let Some(waker) = future.waker.take() {
                    waker.wake();
                }
            }

            tokio::task::yield_now().await;
        }
    });

    let buf = AlignedBuffer::new(1 << 20, 4096);
    let bs = (1 << 20) as u64;
    let mut offset: i64 = 0;
    loop {
        let readed = async_read(&aio_ctx, src_fd, buf.ptr, bs, offset).await;
        if readed <= 0 {
            break;
        }

        async_write(&aio_ctx, dst_fd, buf.ptr, bs, offset).await;
        offset += bs as i64;
    }

    // set right file size
    if src.metadata().unwrap().len() != dst.metadata().unwrap().len() {
        dst.set_len(src.metadata().unwrap().len()).unwrap();
    }

    println!("Copy finished!");
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let local = tokio::task::LocalSet::new();
    local.run_until(
        async_copy("./a.dat", "./b.dat")
    ).await;
}
