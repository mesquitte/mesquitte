use std::{
    cmp, io,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, Sink, Stream};
use pin_project_lite::pin_project;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};
use tungstenite::Message;

struct State {
    read: ReadState,
    write: WriteState,
}

enum ReadState {
    Pending,
    Ready { data: Vec<u8>, amt_read: usize },
    Terminated,
}

enum WriteState {
    Ready,
    Closed,
}

pin_project! {
    pub struct WsByteStream<S> {
        #[pin]
        inner: S,
        state: State,
    }
}

impl<S> WsByteStream<S>
where
    S: Stream<Item = Result<Message, tungstenite::Error>>
        + Sink<Message, Error = tungstenite::Error>
        + Unpin,
{
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            state: State {
                read: ReadState::Pending,
                write: WriteState::Ready,
            },
        }
    }

    fn fill_buf_with_next_msg(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<()>>> {
        let mut this = self.project();
        loop {
            let res = ready!(this.inner.as_mut().poll_next(cx));
            let Some(res) = res else {
                this.state.read = ReadState::Terminated;
                return Poll::Ready(None);
            };
            match res {
                Ok(msg) => match msg {
                    Message::Binary(msg) => {
                        this.state.read = ReadState::Ready {
                            data: msg,
                            amt_read: 0,
                        };
                        return Poll::Ready(Some(Ok(())));
                    }
                    Message::Close(_) => {
                        this.state.read = ReadState::Terminated;
                        return Poll::Ready(None);
                    }
                    _ => continue,
                },
                Err(e) => match e {
                    tungstenite::Error::Io(e) => return Poll::Ready(Some(Err(e))),
                    tungstenite::Error::ConnectionClosed => {
                        this.state.read = ReadState::Terminated;
                        return Poll::Ready(None);
                    }
                    tungstenite::Error::AlreadyClosed => {
                        this.state.read = ReadState::Terminated;
                        let e = io::Error::new(io::ErrorKind::NotConnected, "Already closed");
                        return Poll::Ready(Some(Err(e)));
                    }
                    err => {
                        return Poll::Ready(Some(Err(io::Error::new(io::ErrorKind::Other, err))));
                    }
                },
            }
        }
    }
}

impl<S> AsyncRead for WsByteStream<S>
where
    S: Stream<Item = Result<Message, tungstenite::Error>>
        + Sink<Message, Error = tungstenite::Error>
        + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let this = self.as_mut().project();
            match this.state.read {
                ReadState::Pending => {
                    let res = ready!(self.as_mut().fill_buf_with_next_msg(cx));
                    match res {
                        Some(Ok(())) => continue,
                        Some(Err(e)) => return Poll::Ready(Err(e)),
                        None => continue,
                    }
                }
                ReadState::Ready {
                    ref data,
                    ref mut amt_read,
                } => {
                    let data_in = &data[*amt_read..];
                    let len = cmp::min(buf.remaining(), data_in.len());
                    buf.put_slice(&data_in[..len]);
                    if len == data_in.len() {
                        this.state.read = ReadState::Pending;
                    } else {
                        *amt_read += len;
                    }
                    return Poll::Ready(Ok(()));
                }
                ReadState::Terminated => return Poll::Ready(Ok(())),
            }
        }
    }
}

impl<S> AsyncBufRead for WsByteStream<S>
where
    S: Stream<Item = Result<Message, tungstenite::Error>>
        + Sink<Message, Error = tungstenite::Error>
        + Unpin,
{
    fn poll_fill_buf(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        loop {
            let this = self.as_mut().project();
            match this.state.read {
                ReadState::Pending => {
                    let res = ready!(self.as_mut().fill_buf_with_next_msg(cx));
                    match res {
                        Some(Ok(())) => continue,
                        Some(Err(e)) => return Poll::Ready(Err(e)),
                        None => continue,
                    }
                }
                ReadState::Ready { .. } => {
                    let this = self.project();
                    let ReadState::Ready { ref data, amt_read } = this.state.read else {
                        unreachable!()
                    };
                    return Poll::Ready(Ok(&data[amt_read..]));
                }
                ReadState::Terminated => return Poll::Ready(Ok(&[])),
            }
        }
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        if let ReadState::Ready {
            ref data,
            ref mut amt_read,
        } = self.state.read
        {
            *amt_read = std::cmp::min(data.len(), *amt_read + amt);
            if *amt_read == data.len() {
                self.state.read = ReadState::Pending;
            }
        }
    }
}

impl<S> AsyncWrite for WsByteStream<S>
where
    S: Stream<Item = Result<Message, tungstenite::Error>>
        + Sink<Message, Error = tungstenite::Error>
        + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let mut this = self.project();
        match this.state.write {
            WriteState::Ready => {
                if let Err(e) = ready!(this.inner.as_mut().poll_ready(cx)) {
                    match e {
                        tungstenite::Error::Io(e) => return Poll::Ready(Err(e)),
                        tungstenite::Error::ConnectionClosed => {
                            this.state.write = WriteState::Closed;
                            return Poll::Ready(Ok(0));
                        }
                        tungstenite::Error::AlreadyClosed => {
                            this.state.write = WriteState::Closed;
                            let e = io::Error::new(io::ErrorKind::NotConnected, "Already closed");
                            return Poll::Ready(Err(e));
                        }
                        err => {
                            return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err)));
                        }
                    }
                }
                if let Err(e) = this.inner.as_mut().start_send(Message::Binary(buf.into())) {
                    match e {
                        tungstenite::Error::Io(e) => Poll::Ready(Err(e)),
                        tungstenite::Error::ConnectionClosed => {
                            this.state.write = WriteState::Closed;
                            Poll::Ready(Ok(0))
                        }
                        tungstenite::Error::AlreadyClosed => {
                            this.state.write = WriteState::Closed;
                            let e = io::Error::new(io::ErrorKind::NotConnected, "Already closed");
                            Poll::Ready(Err(e))
                        }
                        err => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err))),
                    }
                } else {
                    this.state.write = WriteState::Ready;
                    Poll::Ready(Ok(buf.len()))
                }
            }
            WriteState::Closed => {
                let e = io::Error::new(io::ErrorKind::NotConnected, "Already closed");
                Poll::Ready(Err(e))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let mut this = self.project();
        if let Err(e) = ready!(this.inner.as_mut().poll_flush(cx)) {
            match e {
                tungstenite::Error::Io(e) => return Poll::Ready(Err(e)),
                tungstenite::Error::ConnectionClosed => {
                    this.state.write = WriteState::Closed;
                    return Poll::Ready(Ok(()));
                }
                tungstenite::Error::AlreadyClosed => {
                    this.state.write = WriteState::Closed;
                    let e = io::Error::new(io::ErrorKind::NotConnected, "Already closed");
                    return Poll::Ready(Err(e));
                }
                err => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err)));
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let mut this = self.project();
        this.state.write = WriteState::Closed;
        if let Err(e) = ready!(this.inner.as_mut().poll_close(cx)) {
            match e {
                tungstenite::Error::Io(e) => return Poll::Ready(Err(e)),
                tungstenite::Error::ConnectionClosed => return Poll::Ready(Ok(())),
                tungstenite::Error::AlreadyClosed => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "Already closed",
                    )))
                }
                err => return Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err))),
            }
        }
        Poll::Ready(Ok(()))
    }
}
