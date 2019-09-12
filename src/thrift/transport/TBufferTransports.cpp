/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cassert>
#include <algorithm>
#include <thrift/thrift_export.h>

#include <thrift/transport/TBufferTransports.h>

using std::string;

namespace apache {
namespace thrift {
namespace transport {

uint32_t TBufferedTransport::readSlow(uint8_t* buf, uint32_t len) {
  auto have = static_cast<uint32_t>(rBound_ - rBase_);

  // We should only take the slow path if we can't satisfy the read
  // with the data already in the buffer.
  assert(have < len);

  // If we have some data in the buffer, copy it out and return it.
  // We have to return it without attempting to read more, since we aren't
  // guaranteed that the underlying transport actually has more data, so
  // attempting to read from it could block.
  if (have > 0) {
    memcpy(buf, rBase_, have);
    setReadBuffer(rBuf_.get(), 0);
    return have;
  }

  // No data is available in our buffer.
  // Get more from underlying transport up to buffer size.
  // Note that this makes a lot of sense if len < rBufSize_
  // and almost no sense otherwise.  TODO(dreiss): Fix that
  // case (possibly including some readv hotness).
  setReadBuffer(rBuf_.get(), transport_->read(rBuf_.get(), rBufSize_));

  // Hand over whatever we have.
  uint32_t give = (std::min)(len, static_cast<uint32_t>(rBound_ - rBase_));
  memcpy(buf, rBase_, give);
  rBase_ += give;

  return give;
}

void TBufferedTransport::writeSlow(const uint8_t* buf, uint32_t len) {
  auto have_bytes = static_cast<uint32_t>(wBase_ - wBuf_.get());
  auto space = static_cast<uint32_t>(wBound_ - wBase_);
  // We should only take the slow path if we can't accommodate the write
  // with the free space already in the buffer.
  assert(wBound_ - wBase_ < static_cast<ptrdiff_t>(len));

  // Now here's the tricky question: should we copy data from buf into our
  // internal buffer and write it from there, or should we just write out
  // the current internal buffer in one syscall and write out buf in another.
  // If our currently buffered data plus buf is at least double our buffer
  // size, we will have to do two syscalls no matter what (except in the
  // degenerate case when our buffer is empty), so there is no use copying.
  // Otherwise, there is sort of a sliding scale.  If we have N-1 bytes
  // buffered and need to write 2, it would be crazy to do two syscalls.
  // On the other hand, if we have 2 bytes buffered and are writing 2N-3,
  // we can save a syscall in the short term by loading up our buffer, writing
  // it out, and copying the rest of the bytes into our buffer.  Of course,
  // if we get another 2-byte write, we haven't saved any syscalls at all,
  // and have just copied nearly 2N bytes for nothing.  Finding a perfect
  // policy would require predicting the size of future writes, so we're just
  // going to always eschew syscalls if we have less than 2N bytes to write.

  // The case where we have to do two syscalls.
  // This case also covers the case where the buffer is empty,
  // but it is clearer (I think) to think of it as two separate cases.
  if ((have_bytes + len >= 2 * wBufSize_) || (have_bytes == 0)) {
    // TODO(dreiss): writev
    if (have_bytes > 0) {
      transport_->write(wBuf_.get(), have_bytes);
    }
    transport_->write(buf, len);
    wBase_ = wBuf_.get();
    return;
  }

  // Fill up our internal buffer for a write.
  memcpy(wBase_, buf, space);
  buf += space;
  len -= space;
  transport_->write(wBuf_.get(), wBufSize_);

  // Copy the rest into our buffer.
  assert(len < wBufSize_);
  memcpy(wBuf_.get(), buf, len);
  wBase_ = wBuf_.get() + len;
  return;
}

const uint8_t* TBufferedTransport::borrowSlow(uint8_t* buf, uint32_t* len) {
  (void)buf;
  (void)len;
  // Simply return NULL.  We don't know if there is actually data available on
  // the underlying transport, so calling read() might block.
  return nullptr;
}

void TBufferedTransport::flush() {
  // Write out any data waiting in the write buffer.
  auto have_bytes = static_cast<uint32_t>(wBase_ - wBuf_.get());
  if (have_bytes > 0) {
    // Note that we reset wBase_ prior to the underlying write
    // to ensure we're in a sane state (i.e. internal buffer cleaned)
    // if the underlying write throws up an exception
    wBase_ = wBuf_.get();
    transport_->write(wBuf_.get(), have_bytes);
  }

  // Flush the underlying transport.
  transport_->flush();
}



void TMemoryBuffer::computeRead(uint32_t len, uint8_t** out_start, uint32_t* out_give) {
  // Correct rBound_ so we can use the fast path in the future.
  rBound_ = wBase_;

  // Decide how much to give.
  uint32_t give = (std::min)(len, available_read());

  *out_start = rBase_;
  *out_give = give;

  // Preincrement rBase_ so the caller doesn't have to.
  rBase_ += give;
}

uint32_t TMemoryBuffer::readSlow(uint8_t* buf, uint32_t len) {
  uint8_t* start;
  uint32_t give;
  computeRead(len, &start, &give);

  // Copy into the provided buffer.
  memcpy(buf, start, give);

  return give;
}

uint32_t TMemoryBuffer::readAppendToString(std::string& str, uint32_t len) {
  // Don't get some stupid assertion failure.
  if (buffer_ == nullptr) {
    return 0;
  }

  uint8_t* start;
  uint32_t give;
  computeRead(len, &start, &give);

  // Append to the provided string.
  str.append((char*)start, give);

  return give;
}

void TMemoryBuffer::ensureCanWrite(uint32_t len) {
  // Check available space
  uint32_t avail = available_write();
  if (len <= avail) {
    return;
  }

  if (!owner_) {
    throw TTransportException("Insufficient space in external MemoryBuffer");
  }

  // Grow the buffer as necessary.
  uint64_t new_size = bufferSize_;
  while (len > avail) {
    new_size = new_size > 0 ? new_size * 2 : 1;
    if (new_size > maxBufferSize_) {
      throw TTransportException(TTransportException::BAD_ARGS,
                                "Internal buffer size overflow");
    }
    avail = available_write() + (static_cast<uint32_t>(new_size) - bufferSize_);
  }

  // Allocate into a new pointer so we don't bork ours if it fails.
  auto* new_buffer = static_cast<uint8_t*>(std::realloc(buffer_, new_size));
  if (new_buffer == nullptr) {
    throw std::bad_alloc();
  }

  rBase_ = new_buffer + (rBase_ - buffer_);
  rBound_ = new_buffer + (rBound_ - buffer_);
  wBase_ = new_buffer + (wBase_ - buffer_);
  wBound_ = new_buffer + new_size;
  buffer_ = new_buffer;
  bufferSize_ = static_cast<uint32_t>(new_size);
}

void TMemoryBuffer::writeSlow(const uint8_t* buf, uint32_t len) {
  ensureCanWrite(len);

  // Copy into the buffer and increment wBase_.
  memcpy(wBase_, buf, len);
  wBase_ += len;
}

void TMemoryBuffer::wroteBytes(uint32_t len) {
  uint32_t avail = available_write();
  if (len > avail) {
    throw TTransportException("Client wrote more bytes than size of buffer.");
  }
  wBase_ += len;
}

const uint8_t* TMemoryBuffer::borrowSlow(uint8_t* buf, uint32_t* len) {
  (void)buf;
  rBound_ = wBase_;
  if (available_read() >= *len) {
    *len = available_read();
    return rBase_;
  }
  return nullptr;
}
}
}
} // apache::thrift::transport
