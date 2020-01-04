// Copyright (c) 2013-2016 Sandstorm Development Group, Inc. and contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//! Asynchronous reading and writing of messages using the
//! [standard stream framing](https://capnproto.org/encoding.html#serialization-over-a-stream).

use std::convert::TryInto;
use std::io;
use std::mem;
use std::pin::Pin;
use std::ptr;
use std::slice;

use capnp::{message, Error, Result, Word, OutputSegments};

use futures::{AsyncBufRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, future};
use futures::task::Poll;

#[inline]
fn ptr_sub<T>(p1: *const T, p2: *const T) -> usize {
    (p1 as usize - p2 as usize) / mem::size_of::<T>()
}

macro_rules! refresh_buffer(
    ($this:expr, $size:ident, $in_ptr:ident, $in_end:ident, $out:ident,
     $outBuf:ident, $buffer_begin:ident) => (
        {
            Pin::new(&mut $this).consume($size);
            let (b, e) = get_read_buffer(&mut $this).await?;
            $in_ptr = b;
            $in_end = e;
            $size = ptr_sub($in_end, $in_ptr);
            $buffer_begin = b;
            if $size == 0 {
                return Err(io::Error::new(io::ErrorKind::Other,
                                          "Premature end of packed input."));
            }
        }
        );
    );

async fn get_read_buffer<R: AsyncBufRead + Unpin>(mut reader: R) -> io::Result<(*const u8, *const u8)> {
    future::poll_fn(|cx| {
        let buf = futures::ready!(Pin::new(&mut reader).poll_fill_buf(cx))?;
        unsafe {
            Poll::Ready(Ok((buf.as_ptr(), buf.get_unchecked(buf.len()) as *const _)))
        }
    }).await
}

async fn packed_read<R: AsyncBufRead + Unpin>(mut inner: R, out_buf: &mut [u8]) -> io::Result<usize> {
    let len = out_buf.len();

    if len == 0 { return Ok(0); }

    assert!(len % 8 == 0, "PackedRead reads must be word-aligned.");

    unsafe {
        let mut out = out_buf.as_mut_ptr();
        let out_end: *mut u8 = out_buf.get_unchecked_mut(len);

        let (mut in_ptr, mut in_end) = get_read_buffer(&mut inner).await?;
        let mut buffer_begin = in_ptr;
        let mut size = ptr_sub(in_end, in_ptr);
        if size == 0 {
            return Ok(0);
        }

        loop {

            let tag: u8;

            assert_eq!(ptr_sub(out, out_buf.as_mut_ptr()) % 8, 0,
                       "Output pointer should always be aligned here.");

            if ptr_sub(in_end, in_ptr) < 10 {
                if out >= out_end {
                    Pin::new(&mut inner).consume(ptr_sub(in_ptr, buffer_begin));
                    return Ok(ptr_sub(out, out_buf.as_mut_ptr()));
                }

                if ptr_sub(in_end, in_ptr) == 0 {
                    refresh_buffer!(inner, size, in_ptr, in_end, out, out_buf, buffer_begin);
                    continue;
                }

                //# We have at least 1, but not 10, bytes available. We need to read
                //# slowly, doing a bounds check on each byte.

                tag = *in_ptr;
                in_ptr = in_ptr.offset(1);

                for i in 0..8 {
                    if (tag & (1u8 << i)) != 0 {
                        if ptr_sub(in_end, in_ptr) == 0 {
                            refresh_buffer!(inner, size, in_ptr, in_end,
                                            out, out_buf, buffer_begin);
                        }
                        *out = *in_ptr;
                        out = out.offset(1);
                        in_ptr = in_ptr.offset(1);
                    } else {
                        *out = 0;
                        out = out.offset(1);
                    }
                }

                if ptr_sub(in_end, in_ptr) == 0 && (tag == 0 || tag == 0xff) {
                    refresh_buffer!(inner, size, in_ptr, in_end,
                                    out, out_buf, buffer_begin);
                }
            } else {
                tag = *in_ptr;
                in_ptr = in_ptr.offset(1);

                for n in 0..8 {
                    let is_nonzero = (tag & (1u8 << n)) != 0;
                    *out = (*in_ptr) & ((-(is_nonzero as i8)) as u8);
                    out = out.offset(1);
                    in_ptr = in_ptr.offset(is_nonzero as isize);
                }
            }
            if tag == 0 {
                assert!(ptr_sub(in_end, in_ptr) > 0,
                        "Should always have non-empty buffer here.");

                let run_length : usize = (*in_ptr) as usize * 8;
                in_ptr = in_ptr.offset(1);

                if run_length > ptr_sub(out_end, out) {
                    return Err(io::Error::new(io::ErrorKind::Other,
                                              "Packed input did not end cleanly on a segment boundary."));
                }

                ptr::write_bytes(out, 0, run_length);
                out = out.offset(run_length as isize);

            } else if tag == 0xff {
                assert!(ptr_sub(in_end, in_ptr) > 0,
                        "Should always have non-empty buffer here");

                let mut run_length : usize = (*in_ptr) as usize * 8;
                in_ptr = in_ptr.offset(1);

                if run_length > ptr_sub(out_end, out) {
                    return Err(io::Error::new(io::ErrorKind::Other,
                                              "Packed input did not end cleanly on a segment boundary."));
                }

                let in_remaining = ptr_sub(in_end, in_ptr);
                if in_remaining >= run_length {
                    //# Fast path.
                    ptr::copy_nonoverlapping(in_ptr, out, run_length);
                    out = out.offset(run_length as isize);
                    in_ptr = in_ptr.offset(run_length as isize);
                } else {
                    //# Copy over the first buffer, then do one big read for the rest.
                    ptr::copy_nonoverlapping(in_ptr, out, in_remaining);
                    out = out.offset(in_remaining as isize);
                    run_length -= in_remaining;

                    Pin::new(&mut inner).consume(size);
                    {
                        let buf = slice::from_raw_parts_mut::<u8>(out, run_length);
                        inner.read_exact(buf).await?;
                    }

                    out = out.offset(run_length as isize);

                    if out == out_end {
                        return Ok(len);
                    } else {
                        let (b, e) = get_read_buffer(&mut inner).await?;
                        in_ptr = b;
                        in_end = e;
                        size = ptr_sub(e, b);
                        buffer_begin = in_ptr;
                        continue;
                    }
                }
            }

            if out == out_end {
                Pin::new(&mut inner).consume(ptr_sub(in_ptr, buffer_begin));
                return Ok(len);
            }
        }
    }
}

async fn packed_read_exact<R: AsyncBufRead + Unpin>(mut reader: R, mut out_buf: &mut [u8]) -> io::Result<()> {
    while !out_buf.is_empty() {
        let n = packed_read(&mut reader, out_buf).await?;
        {
            let (_, rest) = mem::replace(&mut out_buf, &mut []).split_at_mut(n);
            out_buf = rest;
        }
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
    Ok(())
}

pub struct OwnedSegments {
    segment_slices: Vec<(usize, usize)>,
    owned_space: Vec<Word>,
}

impl message::ReaderSegments for OwnedSegments {
    fn get_segment<'a>(&'a self, id: u32) -> Option<&'a [Word]> {
        if id < self.segment_slices.len() as u32 {
            let (a, b) = self.segment_slices[id as usize];
            Some(&self.owned_space[a..b])
        } else {
            None
        }
    }
}

/// Begins an asynchronous read of a message from `reader`.
pub async fn read_message<R>(mut reader: R, options: message::ReaderOptions) -> Result<Option<message::Reader<OwnedSegments>>>
    where R: AsyncBufRead + Unpin
{
    let mut buf: [u8; 8] = [0; 8];
    {
        let n = packed_read(&mut reader, &mut buf[..]).await?;
        if n == 0 {
            return Ok(None);
        } else if n < 8 {
            packed_read_exact(&mut reader, &mut buf[n..]).await?;
        }
    }
    let (segment_count, first_segment_length) = parse_segment_table_first(&buf[..])?;

    let mut segment_slices: Vec<(usize, usize)> = Vec::with_capacity(segment_count);
    segment_slices.push((0,first_segment_length));
    let mut total_words = first_segment_length;

    if segment_count > 1 {
        if segment_count < 4 {
            // small enough that we can reuse our existing buffer
            packed_read_exact(&mut reader, &mut buf).await?;
            for idx in 0..(segment_count - 1) {
                let segment_len =
                    u32::from_le_bytes(buf[(idx * 4)..(idx + 1) * 4].try_into().unwrap()) as usize;

                segment_slices.push((total_words, total_words + segment_len));
                total_words += segment_len;

            }
        } else {
            let mut segment_sizes = vec![0u8; (segment_count & !1) * 4];
            packed_read_exact(&mut reader, &mut segment_sizes[..]).await?;
            for idx in 0..(segment_count - 1) {
                let segment_len =
                    u32::from_le_bytes(segment_sizes[(idx * 4)..(idx + 1) * 4].try_into().unwrap()) as usize;

                segment_slices.push((total_words, total_words + segment_len));
                total_words += segment_len;
            }
        }
    }

    // Don't accept a message which the receiver couldn't possibly traverse without hitting the
    // traversal limit. Without this check, a malicious client could transmit a very large segment
    // size to make the receiver allocate excessive space and possibly crash.
    if total_words as u64 > options.traversal_limit_in_words  {
        return Err(Error::failed(
            format!("Message has {} words, which is too large. To increase the limit on the \
             receiving end, see capnp::message::ReaderOptions.", total_words)))
    }

    let mut owned_space: Vec<Word> = Word::allocate_zeroed_vec(total_words);
    packed_read_exact(&mut reader, Word::words_to_bytes_mut(&mut owned_space[..])).await?;
    let segments = OwnedSegments {segment_slices: segment_slices, owned_space: owned_space};
    Ok(Some(message::Reader::new(segments, options)))
}

/// Parses the first word of the segment table.
///
/// The segment table format for streams is defined in the Cap'n Proto
/// [encoding spec](https://capnproto.org/encoding.html#serialization-over-a-stream)
///
/// Returns the segment count and first segment length, or a state if the
/// read would block.
fn parse_segment_table_first(buf: &[u8]) -> Result<(usize, usize)>
{
    let segment_count = u32::from_le_bytes(buf[0..4].try_into().unwrap()).wrapping_add(1);
    if segment_count >= 512 {
        return Err(Error::failed(format!("Too many segments: {}", segment_count)))
    } else if segment_count == 0 {
        return Err(Error::failed(format!("Too few segments: {}", segment_count)))
    }

    let first_segment_len = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    Ok((segment_count as usize, first_segment_len as usize))
}

/// Something that contains segments ready to be written out.
pub trait AsOutputSegments {
    fn as_output_segments<'a>(&'a self) -> OutputSegments<'a>;
}


impl <'a, M> AsOutputSegments for &'a M where M: AsOutputSegments {
    fn as_output_segments<'b>(&'b self) -> OutputSegments<'b> {
        (*self).as_output_segments()
    }
}

impl <A> AsOutputSegments for message::Builder<A> where A: message::Allocator {
    fn as_output_segments<'a>(&'a self) -> OutputSegments<'a> {
        self.get_segments_for_output()
    }
}

/*impl <'a, A> AsOutputSegments for &'a message::Builder<A> where A: message::Allocator {
    fn as_output_segments<'b>(&'b self) -> OutputSegments<'b> {
        self.get_segments_for_output()
    }
}*/

impl <A> AsOutputSegments for ::std::rc::Rc<message::Builder<A>> where A: message::Allocator {
    fn as_output_segments<'a>(&'a self) -> OutputSegments<'a> {
        self.get_segments_for_output()
    }
}

/// Writes the provided message to `writer`. Does not call `flush()`.
pub async fn write_message<W,M>(mut writer: W, message: M) -> Result<()>
    where W: AsyncWrite + Unpin, M: AsOutputSegments
{
    let segments = message.as_output_segments();
    write_segment_table(&mut writer, &segments[..]).await?;
    write_segments(writer, &segments[..]).await?;
    Ok(())
}

async fn packed_write<W: AsyncWrite + Unpin>(writer: &mut W, in_buf: &[u8]) -> io::Result<usize> {
    unsafe {
        let mut buf_idx: usize = 0;
        let mut buf: [u8; 64] = [0; 64];

        let mut in_ptr: *const u8 = in_buf.get_unchecked(0);
        let in_end: *const u8 = in_buf.get_unchecked(in_buf.len());

        while in_ptr < in_end {

            if buf_idx + 10 > buf.len() {
                //# Oops, we're out of space. We need at least 10
                //# bytes for the fast path, since we don't
                //# bounds-check on every byte.
                writer.write_all(&buf[..buf_idx]).await?;
                buf_idx = 0;
            }

            let tag_pos = buf_idx;
            buf_idx += 1;

            let bit0 = (*in_ptr != 0) as u8;
            *buf.get_unchecked_mut(buf_idx) = *in_ptr;
            buf_idx += bit0 as usize;
            in_ptr = in_ptr.offset(1);

            let bit1 = (*in_ptr != 0) as u8;
            *buf.get_unchecked_mut(buf_idx) = *in_ptr;
            buf_idx += bit1 as usize;
            in_ptr = in_ptr.offset(1);

            let bit2 = (*in_ptr != 0) as u8;
            *buf.get_unchecked_mut(buf_idx) = *in_ptr;
            buf_idx += bit2 as usize;
            in_ptr = in_ptr.offset(1);

            let bit3 = (*in_ptr != 0) as u8;
            *buf.get_unchecked_mut(buf_idx) = *in_ptr;
            buf_idx += bit3 as usize;
            in_ptr = in_ptr.offset(1);

            let bit4 = (*in_ptr != 0) as u8;
            *buf.get_unchecked_mut(buf_idx) = *in_ptr;
            buf_idx += bit4 as usize;
            in_ptr = in_ptr.offset(1);

            let bit5 = (*in_ptr != 0) as u8;
            *buf.get_unchecked_mut(buf_idx) = *in_ptr;
            buf_idx += bit5 as usize;
            in_ptr = in_ptr.offset(1);

            let bit6 = (*in_ptr != 0) as u8;
            *buf.get_unchecked_mut(buf_idx) = *in_ptr;
            buf_idx += bit6 as usize;
            in_ptr = in_ptr.offset(1);

            let bit7 = (*in_ptr != 0) as u8;
            *buf.get_unchecked_mut(buf_idx) = *in_ptr;
            buf_idx += bit7 as usize;
            in_ptr = in_ptr.offset(1);

            let tag: u8 = (bit0 << 0) | (bit1 << 1) | (bit2 << 2) | (bit3 << 3)
                | (bit4 << 4) | (bit5 << 5) | (bit6 << 6) | (bit7 << 7);


            *buf.get_unchecked_mut(tag_pos) = tag;

            if tag == 0 {
                //# An all-zero word is followed by a count of
                //# consecutive zero words (not including the first
                //# one).

                // Here we use our assumption that the input buffer is 8-byte aligned.
                let mut in_word : *const u64 = in_ptr as *const u64;
                let mut limit : *const u64 = in_end as *const u64;
                if ptr_sub(limit, in_word) > 255 {
                    limit = in_word.offset(255);
                }
                while in_word < limit && *in_word == 0 {
                    in_word = in_word.offset(1);
                }

                *buf.get_unchecked_mut(buf_idx) = ptr_sub(in_word, in_ptr as *const u64) as u8;
                buf_idx += 1;
                in_ptr = in_word as *const u8;
            } else if tag == 0xff {
                //# An all-nonzero word is followed by a count of
                //# consecutive uncompressed words, followed by the
                //# uncompressed words themselves.

                //# Count the number of consecutive words in the input
                //# which have no more than a single zero-byte. We look
                //# for at least two zeros because that's the point
                //# where our compression scheme becomes a net win.
                let run_start = in_ptr;
                let mut limit = in_end;
                if ptr_sub(limit, in_ptr) > 255 * 8 {
                    limit = in_ptr.offset(255 * 8);
                }

                while in_ptr < limit {
                    let mut c = 0;

                    for _ in 0..8 {
                        c += (*in_ptr == 0) as u8;
                        in_ptr = in_ptr.offset(1);
                    }

                    if c >= 2 {
                        //# Un-read the word with multiple zeros, since
                        //# we'll want to compress that one.
                        in_ptr = in_ptr.offset(-8);
                        break;
                    }
                }

                let count: usize = ptr_sub(in_ptr, run_start);
                *buf.get_unchecked_mut(buf_idx) = (count / 8) as u8;
                buf_idx += 1;

                writer.write_all(&buf[..buf_idx]).await?;
                buf_idx = 0;
                writer.write_all(slice::from_raw_parts::<u8>(run_start, count)).await?;
            }
        }

        writer.write_all(&buf[..buf_idx]).await?;
        Ok(in_buf.len())
    }
}

async fn packed_write_all<W: AsyncWrite + Unpin>(writer: &mut W, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        let n = packed_write(writer, buf).await?;
        {
            let (_, rest) = mem::replace(&mut buf, &[]).split_at(n);
            buf = rest;
        }
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
    }
    Ok(())
}

async fn write_segment_table<W>(mut write: W, segments: &[&[Word]]) -> ::std::io::Result<()>
where W: AsyncWrite + Unpin
{
    let mut buf: [u8; 8] = [0; 8];
    let segment_count = segments.len();

    // write the first Word, which contains segment_count and the 1st segment length
    buf[0..4].copy_from_slice(&(segment_count as u32 - 1).to_le_bytes());
    buf[4..8].copy_from_slice(&(segments[0].len() as u32).to_le_bytes());
    packed_write_all(&mut write, &buf).await?;

    if segment_count > 1 {
        if segment_count < 4 {
            for idx in 1..segment_count {
                buf[(idx - 1) * 4..idx * 4].copy_from_slice(
                    &(segments[idx].len() as u32).to_le_bytes());
            }
            if segment_count == 2 {
                for idx in 4..8 { buf[idx] = 0 }
            }
            packed_write_all(&mut write, &buf).await?;
        } else {
            let mut buf = vec![0; (segment_count & !1) * 4];
            for idx in 1..segment_count {
                buf[(idx - 1) * 4..idx * 4].copy_from_slice(
                    &(segments[idx].len() as u32).to_le_bytes());
            }
            if segment_count % 2 == 0 {
                for idx in (buf.len() - 4)..(buf.len()) { buf[idx] = 0 }
            }
            packed_write_all(&mut write, &buf).await?;
        }
    }
    Ok(())
}

/// Writes segments to `write`.
async fn write_segments<W>(mut write: W, segments: &[&[Word]]) -> Result<()>
    where W: AsyncWrite + Unpin
{
    for i in 0..segments.len() {
        packed_write_all(&mut write, Word::words_to_bytes(segments[i])).await?;
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use quickcheck::{quickcheck, TestResult};

    use capnp::{Word};
    use capnp::message::{ReaderOptions};
    use super::{read_message, write_segment_table, write_segments};
    use futures::{AsyncWrite};
    use futures::io::Cursor;
    use crate::serialize_packed::{packed_read_exact, packed_read, packed_write};

    /// Writes segments as if they were a Capnproto message.
    pub async fn write_message_segments<W>(mut write: &mut W, segments: &Vec<Vec<Word>>) where W: AsyncWrite + Unpin {
        let borrowed_segments: &[&[Word]] = &segments.iter()
            .map(|segment| &segment[..])
            .collect::<Vec<_>>()[..];
        write_segment_table(&mut write, borrowed_segments).await.unwrap();
        write_segments(write, borrowed_segments).await.unwrap();
    }

    pub async fn check_unpacks_to(mut packed: &[u8], unpacked: &[u8]) {
        let mut bytes: Vec<u8> = vec![0; unpacked.len()];
        packed_read_exact(&mut packed, &mut bytes[..]).await.unwrap();

        let mut buf = [0; 8];
        let res = packed_read(&mut packed, &mut buf).await.unwrap();
        assert_eq!(res, 0); // EOF
        assert_eq!(bytes, unpacked);
    }

    pub fn check_packing(unpacked_unaligned: &[u8], packed: &[u8]) {
        // We need to make sure the unpacked bytes are aligned before
        // we pass them to a `PackedWrite`.
        let mut unpacked_words = Word::allocate_zeroed_vec(unpacked_unaligned.len() / 8);
        Word::words_to_bytes_mut(&mut unpacked_words).copy_from_slice(unpacked_unaligned);
        let unpacked = Word::words_to_bytes(&unpacked_words);

        // --------
        // write

        let mut bytes: Vec<u8> = vec![0; packed.len()];
        {
            let mut cursor = Cursor::new(bytes);
            futures::executor::block_on(Box::pin(packed_write(&mut cursor, unpacked))).unwrap();
            bytes = cursor.into_inner();
        }

        assert_eq!(bytes, packed);

        // --------
        // read
        futures::executor::block_on(check_unpacks_to(packed, unpacked));
    }

    #[test]
    pub fn simple_packing() {
        check_packing(&[], &[]);
        check_packing(&[0; 8], &[0,0]);
        check_packing(&[0,0,12,0,0,34,0,0], &[0x24,12,34]);
        check_packing(&[1,3,2,4,5,7,6,8], &[0xff,1,3,2,4,5,7,6,8,0]);
        check_packing(&[0,0,0,0,0,0,0,0,1,3,2,4,5,7,6,8], &[0,0,0xff,1,3,2,4,5,7,6,8,0]);
        check_packing(&[0,0,12,0,0,34,0,0,1,3,2,4,5,7,6,8], &[0x24,12,34,0xff,1,3,2,4,5,7,6,8,0]);
        check_packing(&[1,3,2,4,5,7,6,8,8,6,7,4,5,2,3,1], &[0xff,1,3,2,4,5,7,6,8,1,8,6,7,4,5,2,3,1]);

        check_packing(
            &[1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8, 0,2,4,0,9,0,5,1],
            &[0xff,1,2,3,4,5,6,7,8, 3, 1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8,
                0xd6,2,4,9,5,1]);
        check_packing(
            &[1,2,3,4,5,6,7,8, 1,2,3,4,5,6,7,8, 6,2,4,3,9,0,5,1, 1,2,3,4,5,6,7,8, 0,2,4,0,9,0,5,1],
            &[0xff,1,2,3,4,5,6,7,8, 3, 1,2,3,4,5,6,7,8, 6,2,4,3,9,0,5,1, 1,2,3,4,5,6,7,8,
                0xd6,2,4,9,5,1]);

        check_packing(
            &[8,0,100,6,0,1,1,2, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,1,0,2,0,3,1],
            &[0xed,8,100,6,1,1,2, 0,2, 0xd4,1,2,3,1]);

        check_packing(&[0; 16], &[0,1]);
        check_packing(&[0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0], &[0,2]);
    }

    #[test]
    fn check_round_trip() {
        fn round_trip(segments: Vec<Vec<Word>>) -> TestResult {
            use capnp::message::ReaderSegments;
            if segments.len() == 0 { return TestResult::discard(); }
            let mut cursor = Cursor::new(Vec::new());

            futures::executor::block_on(write_message_segments(&mut cursor, &segments));
            cursor.set_position(0);
            let message = futures::executor::block_on(Box::pin(read_message(&mut cursor, ReaderOptions::new()))).unwrap();
            let result_segments = message.unwrap().into_segments();

            TestResult::from_bool(segments.iter().enumerate().all(|(i, segment)| {
                &segment[..] == result_segments.get_segment(i as u32).unwrap()
            }))
        }

        quickcheck(round_trip as fn(Vec<Vec<Word>>) -> TestResult);
    }

    #[test]
    fn check_roundtrip_empty() {
        let segments = vec!(vec![], vec![], vec![], vec![]);
        use capnp::message::ReaderSegments;
        let mut cursor = Cursor::new(Vec::new());

        futures::executor::block_on(write_message_segments(&mut cursor, &segments));
        cursor.set_position(0);
        let message = futures::executor::block_on(Box::pin(read_message(&mut cursor, ReaderOptions::new()))).unwrap();
        let result_segments = message.unwrap().into_segments();
        segments.iter().enumerate().all(|(i, segment)| {
            assert_eq!(&segment[..], result_segments.get_segment(i as u32).unwrap());
            true
        });
    }

    #[test]
    fn fuzz_unpack() {
        fn unpack(packed: Vec<u8>) -> TestResult {

            let len = packed.len();
            let mut out_buffer: Vec<u8> = vec![0; len * 8];

            let _ = futures::executor::block_on(Box::pin(packed_read_exact(&mut &packed[..], &mut out_buffer)));
            TestResult::from_bool(true)
        }

        quickcheck(unpack as fn(Vec<u8>) -> TestResult);
    }

    #[test]
    fn did_not_end_cleanly_on_a_segment_boundary() {
        let packed = &[0xff, 1, 2, 3, 4, 5, 6, 7, 8, 37, 1, 2];

        let mut bytes: Vec<u8> = vec![0; 200];
        match futures::executor::block_on(Box::pin(packed_read_exact(&mut &packed[..], &mut bytes[..]))) {
            Ok(_) => panic!("should have been an error"),
            Err(e) => {
                assert_eq!(::std::error::Error::description(&e),
                           "Packed input did not end cleanly on a segment boundary.");
            }
        }
    }

    #[test]
    fn premature_end_of_packed_input() {
        fn helper(mut packed: &[u8]) {
            let mut bytes: Vec<u8> = vec![0; 200];
            match futures::executor::block_on(Box::pin(packed_read_exact(&mut packed, &mut bytes[..]))) {
                Ok(_) => panic!("should have been an error"),
                Err(e) => {
                    assert_eq!(::std::error::Error::description(&e), "Premature end of packed input.");
                }
            }
        }

        helper(&[0xf0, 1, 2]);
        helper(&[0]);
        helper(&[0xff, 1, 2, 3, 4, 5, 6, 7, 8]);

        // In this case, the error is only due to the fact that the unpacked data does not
        // fill up the given output buffer.
        helper(&[1, 1]);
    }

    #[test]
    fn packed_segment_table() {
        let packed_buf = &[0x11, 4, 1, 0, 1, 0, 0];

        futures::executor::block_on(check_unpacks_to(
            packed_buf,
            &[4, 0, 0, 0, 1, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0]));

        let mut cursor = Cursor::new(packed_buf);
        // At one point, this failed due to serialize::read_message()
        // reading the segment table only one word at a time.
        futures::executor::block_on(Box::pin(read_message(&mut cursor, Default::default()))).unwrap();
    }
}
