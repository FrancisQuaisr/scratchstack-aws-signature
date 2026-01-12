use {
    bytes::Bytes,
    http_body::{Body, Frame},
    http_body_util::BodyExt as _,
    std::{
        collections::VecDeque,
        marker::PhantomData,
        pin::Pin,
        task::{Context, Poll},
    },
};

pub(crate) struct StoredBody<D, E> {
    _error_type: PhantomData<E>,
    frames: VecDeque<Frame<D>>,
    data_length: usize,
}

impl<D, E> StoredBody<D, E>
where
    D: bytes::Buf,
{
    pub async fn try_from_async<B: Body<Data = D, Error = E>>(body: B) -> Result<StoredBody<D, E>, E> {
        let mut body = Box::pin(body);
        let mut frames = VecDeque::new();
        let mut data_length = 0;
        while let Some(frame) = body.frame().await {
            let frame = frame?;
            data_length += frame.data_ref().map(|d| d.remaining()).unwrap_or_default();
            frames.push_back(frame);
        }

        Ok(StoredBody {
            _error_type: PhantomData,
            data_length,
            frames,
        })
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(self.data_length);
        for frame in &self.frames {
            if let Some(data) = frame.data_ref() {
                bytes.extend_from_slice(data.chunk());
            }
        }
        Bytes::from(bytes)
    }
}

impl<D, E> Body for StoredBody<D, E>
where
    D: bytes::Buf + Unpin,
    E: Unpin,
{
    type Data = D;
    type Error = E;

    fn poll_frame(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Result<http_body::Frame<D>, E>>> {
        let self_mut = self.get_mut();
        if let Some(frame) = self_mut.frames.pop_front() {
            Poll::Ready(Some(Ok(frame)))
        } else {
            Poll::Ready(None)
        }
    }
}
