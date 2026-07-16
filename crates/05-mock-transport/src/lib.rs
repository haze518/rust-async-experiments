use std::{
    collections::{HashMap, VecDeque},
    io,
    mem::MaybeUninit,
    pin::Pin,
    sync::{Arc, Mutex, atomic::AtomicUsize},
    task::{Context, Poll, Wake, Waker},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};

struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

struct WaitTable {
    next: AtomicUsize,
    entries: HashMap<usize, Entry>,
}

impl WaitTable {
    fn add(&mut self) -> usize {
        let next = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.entries.insert(
            next,
            Entry {
                waker: None,
                result: None,
                finished: false,
            },
        );
        next
    }
}

struct Entry {
    waker: Option<Waker>,
    result: Option<Bytes>,
    finished: bool,
}

struct DriverFuture {
    id: usize,
    table: Arc<Mutex<WaitTable>>,
}

impl Future for DriverFuture {
    type Output = Option<Bytes>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut table = self.table.lock().unwrap();
        match table.entries.remove(&self.id) {
            Some(mut entry) => {
                if entry.finished {
                    return Poll::Ready(entry.result);
                }
                entry.waker = Some(cx.waker().clone());
                table.entries.insert(self.id, entry);
                Poll::Pending
            }
            None => Poll::Ready(None),
        }
    }
}

enum DriverCommand {
    Data(Bytes),
    Close,
}

struct Driver {
    state: Arc<Mutex<DriverState>>,
    table: Arc<Mutex<WaitTable>>,
}

impl Future for Driver {
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (keep_going, maybe_buf, closed) = {
            let mut state = self.state.lock().unwrap();

            let mut keep_going = state.drive_commands();
            keep_going |= state.drive_write(cx)?;
            let (ok, buf) = state.drive_read(cx)?;
            keep_going |= ok;

            let closed = state.closed;

            state.waker = Some(cx.waker().clone());

            (keep_going, buf, closed)
        };

        let waker = {
            let mut table = self.table.lock().unwrap();

            if let Some(buf) = maybe_buf {
                let id = table.entries.keys().min().copied();

                if let Some(id) = id
                    && let Some(entry) = table.entries.get_mut(&id)
                {
                    entry.result = Some(buf);
                    entry.finished = true;
                    entry.waker.take()
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(waker) = waker {
            waker.wake();
        }

        if closed {
            return Poll::Ready(Ok(()));
        }

        if keep_going {
            cx.waker().wake_by_ref();
        }

        Poll::Pending
    }
}
impl Driver {}

struct DriverState {
    queue: VecDeque<DriverCommand>,
    waker: Option<Waker>,
    closed: bool,

    transport: Transport,
    outgoing_buf: VecDeque<Bytes>,
    current_send: Option<Bytes>,
    send_offset: usize,
    current_read: BytesMut,
}

impl DriverState {
    fn enqueue(&mut self, command: DriverCommand) {
        self.queue.push_back(command);
    }

    fn dequeue(&mut self) -> Option<DriverCommand> {
        self.queue.pop_front()
    }

    fn drive_commands(&mut self) -> bool {
        if self.queue.len() == 0 {
            return false;
        }
        for command in self.queue.drain(..) {
            match command {
                DriverCommand::Data(d) => {
                    self.outgoing_buf.push_back(d);
                }
                DriverCommand::Close => {
                    self.closed = true;
                    return false;
                }
            }
        }
        true
    }

    fn drive_write(&mut self, cx: &mut Context<'_>) -> io::Result<bool> {
        if self.current_send.is_none() {
            self.current_send = self.outgoing_buf.pop_front();
            self.send_offset = 0;
        }

        let buf = match &self.current_send {
            Some(b) => b.clone(),
            None => return Ok(false),
        };
        let transport = &mut self.transport;
        let mut offset = self.send_offset;
        while offset < buf.len() {
            let written =
                match Pin::new(&mut *transport).poll_write(cx, &buf[self.send_offset..])? {
                    Poll::Ready(0) => {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "write returned 0 bytes",
                        ));
                    }
                    Poll::Ready(n) => n,
                    Poll::Pending => return Ok(false),
                };
            offset += written;
            self.send_offset += written;
        }

        match Pin::new(transport).poll_flush(cx)? {
            Poll::Ready(()) => {}
            Poll::Pending => return Ok(false),
        }

        self.send_offset = 0;
        self.current_send = None;
        Ok(true)
    }

    fn drive_read(&mut self, cx: &mut Context<'_>) -> io::Result<(bool, Option<Bytes>)> {
        let mut progress = false;

        loop {
            if self.current_read.spare_capacity_mut().is_empty() {
                self.current_read.reserve(1024);
            }

            let spare: &mut [MaybeUninit<u8>] = self.current_read.spare_capacity_mut();

            let buf: &mut [u8] = unsafe { &mut *(spare as *mut [MaybeUninit<u8>] as *mut [u8]) };

            let n = {
                let transport = &mut self.transport;

                match Pin::new(transport).poll_read(cx, buf)? {
                    Poll::Ready(0) => {
                        // TODO close ConnectionRefused
                        return Ok((true, None));
                    }
                    Poll::Ready(s) => s,
                    Poll::Pending => break,
                }
            };

            unsafe {
                self.current_read.advance_mut(n);
            }

            progress = true;
        }

        if progress {
            let current = std::mem::take(&mut self.current_read).freeze();
            return Ok((true, Some(current)));
        }

        Ok((false, None))
    }
}

struct ClientHandle {
    state: Arc<Mutex<DriverState>>,
    table: Arc<Mutex<WaitTable>>,
}

impl ClientHandle {
    fn send(&self, data: Bytes) -> Result<DriverFuture, io::Error> {
        let (id, waker) = {
            let mut state = self.state.lock().unwrap();
            let mut table = self.table.lock().unwrap();
            if state.closed {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "driver is closed",
                ));
            }

            state.enqueue(DriverCommand::Data(data));
            let id = table.add();
            let waker = state.waker.take();
            (id, waker)
        };

        if let Some(waker) = waker {
            waker.wake();
        }

        Ok(DriverFuture {
            id,
            table: self.table.clone(),
        })
    }
}

#[derive(PartialEq)]
enum TransportState {
    Idle,
    Writting,
    Closed,
}

struct TransportSharedState {
    waker: Option<Waker>,
    state: TransportState,
    incoming_read_buf: BytesMut,
    pending_write_buf: BytesMut,
    flushed_out_buf: BytesMut,
}

struct Transport {
    state: Arc<Mutex<TransportSharedState>>,
    max_per_request: usize,
    max_buf_size: usize,
}

impl Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut guard = self.state.lock().unwrap();
        match guard.state {
            TransportState::Idle => {
                let n = buf.len().min(guard.incoming_read_buf.len());
                if n == 0 {
                    guard.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                buf[..n].copy_from_slice(&guard.incoming_read_buf[..n]);
                guard.incoming_read_buf.advance(n);
                Poll::Ready(Ok(n))
            }
            TransportState::Closed => {
                let n = buf.len().min(guard.incoming_read_buf.len());
                buf[..n].copy_from_slice(&guard.incoming_read_buf[..n]);
                guard.incoming_read_buf.advance(n);
                Poll::Ready(Ok(n))
            }
            TransportState::Writting => {
                guard.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut guard = self.state.lock().unwrap();

        match guard.state {
            TransportState::Idle => {
                if guard.pending_write_buf.len() >= self.max_buf_size {
                    guard.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }

                let available = self.max_buf_size - guard.pending_write_buf.len();
                let n = std::cmp::min(buf.len(), available).min(self.max_per_request);
                if n > 0 {
                    guard.pending_write_buf.put_slice(&buf[..n]);
                    return Poll::Ready(Ok(n));
                }
                guard.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            TransportState::Closed => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "transport is closed",
            ))),
            TransportState::Writting => {
                guard.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        {
            let mut guard = self.state.lock().unwrap();
            if guard.state != TransportState::Idle {
                return Poll::Ready(Err(io::Error::other("transport is busy")));
            }
            guard.state = TransportState::Writting;
        }

        let mut guard = self.state.lock().unwrap();
        let len = guard.pending_write_buf.len();
        let pending = guard.pending_write_buf.split_to(len);
        guard.flushed_out_buf.extend_from_slice(&pending);
        guard.state = TransportState::Idle;
        Poll::Ready(Ok(()))
    }

    fn poll_close(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut guard = self.state.lock().unwrap();
        if guard.state == TransportState::Closed {
            return Poll::Ready(Ok(()));
        }

        guard.state = TransportState::Closed;
        Poll::Ready(Ok(()))
    }
}

pub fn crate_name() -> &'static str {
    "mock-transport"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noop_cx() -> Context<'static> {
        let waker = Waker::from(Arc::new(NoopWaker));
        Context::from_waker(Box::leak(Box::new(waker)))
    }

    #[test]
    fn test_happy() {
        let transprot_state = Arc::new(Mutex::new(TransportSharedState {
            waker: None,
            state: TransportState::Idle,
            incoming_read_buf: BytesMut::new(),
            pending_write_buf: BytesMut::new(),
            flushed_out_buf: BytesMut::new(),
        }));

        let transport = Transport {
            state: transprot_state.clone(),
            max_per_request: 1024,
            max_buf_size: 8192,
        };
        let driver_state = Arc::new(Mutex::new(DriverState {
            queue: VecDeque::new(),
            waker: None,
            closed: false,
            transport,
            outgoing_buf: VecDeque::new(),
            current_send: None,
            send_offset: 0,
            current_read: BytesMut::new(),
        }));
        let wait_table = Arc::new(Mutex::new(WaitTable {
            next: AtomicUsize::new(0),
            entries: HashMap::new(),
        }));
        let mut driver = Driver {
            state: driver_state.clone(),
            table: wait_table.clone(),
        };
        let client_handle = ClientHandle {
            state: driver_state.clone(),
            table: wait_table.clone(),
        };

        let mut cx = noop_cx();
        let mut fut = client_handle.send(Bytes::from_static(b"Hello")).unwrap();

        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));

        assert!(matches!(Pin::new(&mut driver).poll(&mut cx), Poll::Pending));
        {
            let mut ts = transprot_state.lock().unwrap();
            ts.incoming_read_buf.put_slice(b"Hello there");
        }
        assert!(matches!(Pin::new(&mut driver).poll(&mut cx), Poll::Pending));
        match Pin::new(&mut fut).poll(&mut cx) {
            Poll::Ready(Some(bytes)) => {
                assert_eq!(bytes, Bytes::from_static(b"Hello there"));
            }
            other => panic!("unexpected poll result: {other:?}"),
        }
    }
}
